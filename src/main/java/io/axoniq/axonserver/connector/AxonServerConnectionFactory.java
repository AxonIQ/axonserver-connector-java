/*
 * Copyright (c) 2010-2020. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector;

import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.ContextAddingInterceptor;
import io.axoniq.axonserver.connector.impl.ContextConnection;
import io.axoniq.axonserver.connector.impl.GrpcBufferingInterceptor;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.connector.impl.TokenAddingInterceptor;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * The component which manages all the connections which an Axon client can establish with an Axon Server instance.
 * Does so by creating {@link Channel}s per context and providing them as the means to dispatch/receive messages.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class AxonServerConnectionFactory {

    private static final Logger logger = LoggerFactory.getLogger(AxonServerConnectionFactory.class);
    private static final String CONNECTOR_VERSION = "4.4";
    private final Map<String, String> tags = new HashMap<>();
    private final String applicationName;
    private final String clientInstanceId;

    private final Map<String, ContextConnection> connections = new ConcurrentHashMap<>();
    private final List<ServerAddress> routingServers;
    private final long connectTimeout;
    private final String token;
    private final ScheduledExecutorService executorService;
    private final Function<NettyChannelBuilder, ManagedChannelBuilder<?>> connectionConfig;
    private final boolean forcePlatformReconnect;
    private final long reconnectInterval;
    private final boolean suppressDownloadMessage;

    private volatile boolean shutdown;

    protected AxonServerConnectionFactory(Builder builder) {
        this.applicationName = builder.applicationName;
        this.clientInstanceId = builder.clientInstanceId;
        this.connectTimeout = builder.connectTimeout;
        this.token = builder.token;
        this.executorService = builder.executorService;
        this.suppressDownloadMessage = builder.suppressDownloadMessage;
        this.routingServers = builder.routingServers;
        this.connectionConfig = builder.sslConfig
                .andThen(builder.keepAliveConfig)
                .andThen(builder.otherConfig);
        this.forcePlatformReconnect = builder.forcePlatformReconnect;
        this.reconnectInterval = builder.reconnectInterval;
    }

    public static Builder forClient(String applicationName) {
        return forClient(applicationName, UUID.randomUUID().toString());
    }

    public static Builder forClient(String applicationName, String clientInstanceId) {
        return new AxonServerConnectionFactory.Builder(applicationName, clientInstanceId);
    }


    public AxonServerConnection connect(String context) {
        if (shutdown) {
            throw new IllegalStateException("Connector is already shut down");
        }

        ContextConnection contextConnection = connections.computeIfAbsent(context, this::createConnection);
        contextConnection.connect();
        return contextConnection;
    }

    private ContextConnection createConnection(String context) {
        ClientIdentification clientIdentification =
                ClientIdentification.newBuilder()
                                    .setClientId(clientInstanceId)
                                    .setComponentName(applicationName)
                                    .putAllTags(tags)
                                    .setVersion(CONNECTOR_VERSION)
                                    .build();

        return new ContextConnection(clientIdentification, executorService,
                                     new AxonServerManagedChannel(routingServers,
                                                                  connectTimeout, reconnectInterval, TimeUnit.MILLISECONDS,
                                                                  context,
                                                                  clientIdentification,
                                                                  executorService,
                                                                  forcePlatformReconnect,
                                                                  this::createChannel),
                                     context);
    }

    private ManagedChannel createChannel(ServerAddress address, String context) {
        ManagedChannelBuilder<?> builder = connectionConfig.apply(NettyChannelBuilder.forAddress(address.hostName(), address.grpcPort()));

        if (!suppressDownloadMessage) {
            builder.intercept(new DowloadInstructionInterceptor());
        }

        return builder.intercept(new GrpcBufferingInterceptor(50),
                                  new ContextAddingInterceptor(context),
                                  new TokenAddingInterceptor(token)
                ).build();
    }

    public void shutdown() {
        shutdown = true;
        connections.forEach((k, conn) -> conn.disconnect());
        executorService.shutdown();
        try {
            executorService.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (!executorService.isTerminated()) {
            logger.warn("Forcefully shutting down executor service.");
            executorService.shutdownNow();
        }
    }

    public static class Builder {
        private final String applicationName;
        private final String clientInstanceId;
        private final Map<String, String> tags = new HashMap<>();
        private List<ServerAddress> routingServers;
        private long connectTimeout = 10000;
        private String token;
        private boolean suppressDownloadMessage = false;
        private ScheduledExecutorService executorService;
        private Function<NettyChannelBuilder, NettyChannelBuilder> sslConfig = NettyChannelBuilder::usePlaintext;
        private Function<NettyChannelBuilder, NettyChannelBuilder> keepAliveConfig = UnaryOperator.identity();
        private Function<ManagedChannelBuilder<?>, ManagedChannelBuilder<?>> otherConfig = UnaryOperator.identity();
        private boolean forcePlatformReconnect = true;
        private long reconnectInterval = 2000;
        private int reconnectExecutorPoolsize = 2;

        public Builder(String applicationName, String clientInstanceId) {
            this.applicationName = applicationName;
            this.clientInstanceId = clientInstanceId;
        }

        public Builder routingServers(ServerAddress... serverAddresses) {
            suppressDownloadMessage = true;
            this.routingServers = new ArrayList<>(Arrays.asList(serverAddresses));
            return this;
        }

        public Builder reconnectInterval(long interval, TimeUnit timeUnit) {
            this.reconnectInterval = timeUnit.toMillis(interval);
            return this;
        }

        public Builder connectTimeout(long timeout, TimeUnit timeUnit) {
            this.connectTimeout = timeUnit.toMillis(timeout);
            return this;
        }

        public Builder clientTags(Map<String, String> additionalTags) {
            this.tags.putAll(additionalTags);
            return this;
        }

        public Builder clientTag(String key, String value) {
            this.tags.put(key, value);
            return this;
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public Builder useTransportSecurity() {
            sslConfig = NettyChannelBuilder::useTransportSecurity;
            return this;
        }

        public Builder useTransportSecurity(SslContext sslContext) {
            sslConfig = cb -> cb.sslContext(sslContext);
            return this;
        }

        public Builder forcePlatformReconnect(boolean forcePlatformReconnect) {
            this.forcePlatformReconnect = forcePlatformReconnect;
            return this;
        }

        public Builder reconnectorThreadPoolSize(int poolSize) {
            this.reconnectExecutorPoolsize = poolSize;
            return this;
        }

        /**
         * Sets the time without read activity before sending a keepalive ping. An unreasonably small value might be
         * increased, and Long.MAX_VALUE nano seconds or an unreasonably large value will disable keepalive.
         * Defaults to infinite.
         * <p>
         * If no read activity is recorded within the given timeout after sending a keepalive ping, the connection is
         * considered dead.
         * <p>
         * Clients must receive permission from the service owner before enabling this option. Keepalives can increase
         * the
         * load on services and are commonly "invisible" making it hard to notice when they are causing excessive load.
         * Clients are strongly encouraged to use only as small of a value as necessary.
         *
         * @param interval time without read activity before sending a keepalive ping
         * @param timeout  the time waiting for read activity after sending a keepalive ping
         * @param timeUnit the unit in which the interval and timeout are expressed
         *
         * @return this instance
         */
        public Builder usingKeepAlive(long interval, long timeout, TimeUnit timeUnit, boolean keepAliveWithoutCalls) {
            keepAliveConfig = cb -> cb.keepAliveTime(interval, timeUnit)
                                      .keepAliveTimeout(timeout, timeUnit)
                                      .keepAliveWithoutCalls(keepAliveWithoutCalls);
            return this;
        }

        public Builder maxInboundMessageSize(int bytes) {
            otherConfig = otherConfig.andThen(cb -> cb.maxInboundMessageSize(bytes));
            return this;
        }

        public Builder customize(Function<ManagedChannelBuilder<?>, ManagedChannelBuilder<?>> customization) {
            otherConfig = otherConfig.andThen(customization);
            return this;
        }

        protected void validate() {
            if (routingServers == null) {
                routingServers = Collections.singletonList(new ServerAddress());
            }
            if (executorService == null) {
                executorService = new ScheduledThreadPoolExecutor(reconnectExecutorPoolsize, AxonConnectorThreadFactory.forInstanceId(clientInstanceId));
            }
        }

        public AxonServerConnectionFactory build() {
            validate();
            return new AxonServerConnectionFactory(this);
        }

    }

    private static class DowloadInstructionInterceptor implements ClientInterceptor {

        private volatile boolean suppressDownloadMessage = false;

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            if (!suppressDownloadMessage || "io.axoniq.axonserver.grpc.control.PlatformService/GetPlatformServer".equals(method.getFullMethodName())) {
                return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                            @Override
                            public void onClose(Status status, Metadata trailers) {
                                if (status.getCode() == Status.Code.UNAVAILABLE) {
                                    writeDownloadMessage();
                                }
                                super.onClose(status, trailers);
                            }
                        }, headers);
                    }
                };

            }
            return next.newCall(method, callOptions);
        }

        private synchronized void writeDownloadMessage() {
            if (!suppressDownloadMessage) {
                suppressDownloadMessage = true;
                try (InputStream in = getClass().getClassLoader().getResourceAsStream("axonserver_download.txt")) {
                    byte[] buffer = new byte[1024];
                    int read;
                    while (in != null && (read = in.read(buffer, 0, 1024)) >= 0) {
                        System.out.write(buffer, 0, read);
                    }
                } catch (IOException e) {
                    logger.debug("Unable to write download advice. You're on your own now.", e);
                }
            } else {
                suppressDownloadMessage = true;
            }
        }
    }
}
