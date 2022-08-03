/*
 * Copyright (c) 2020-2021. AxonIQ
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

import io.axoniq.axonserver.connector.impl.AxonConnectorThreadFactory;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.ContextConnection;
import io.axoniq.axonserver.connector.impl.GrpcBufferingInterceptor;
import io.axoniq.axonserver.connector.impl.HeaderAttachingInterceptor;
import io.axoniq.axonserver.connector.impl.Headers;
import io.axoniq.axonserver.connector.impl.ReconnectConfiguration;
import io.axoniq.axonserver.connector.impl.ServerAddress;
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
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.randomHex;

/**
 * The component which manages all the connections which an Axon client can establish with an Axon Server instance. Does
 * so by creating {@link Channel}s per context and providing them as the means to dispatch/receive messages.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class AxonServerConnectionFactory {

    private static final Logger logger = LoggerFactory.getLogger(AxonServerConnectionFactory.class);

    private static final String CONNECTOR_VERSION = "4.4";

    private final Map<String, String> tags = new HashMap<>();
    private final String componentName;
    private final String clientInstanceId;

    private final Map<String, ContextConnection> connections = new ConcurrentHashMap<>();
    private final List<ServerAddress> routingServers;
    private final String token;
    private final ScheduledExecutorService executorService;
    private final Function<NettyChannelBuilder, ManagedChannelBuilder<?>> connectionConfig;
    private final boolean suppressDownloadMessage;
    private final ReconnectConfiguration reconnectConfiguration;
    private final long processorInfoUpdateFrequency;
    private final int commandPermits;
    private final int queryPermits;
    private volatile boolean shutdown;

    /**
     * Instantiates an {@link AxonServerConnectionFactory} with the given {@code builder}.
     *
     * @param builder the {@link Builder} used to set all the specifics of an {@link AxonServerConnectionFactory}
     */
    protected AxonServerConnectionFactory(Builder builder) {
        this.componentName = builder.componentName;
        this.clientInstanceId = builder.clientInstanceId;
        this.token = builder.token;
        this.tags.putAll(builder.tags);
        this.executorService = builder.executorService;
        this.suppressDownloadMessage = builder.suppressDownloadMessage;
        this.routingServers = builder.routingServers;
        this.connectionConfig = builder.sslConfig
                .andThen(builder.keepAliveConfig)
                .andThen(builder.otherConfig);
        this.reconnectConfiguration = new ReconnectConfiguration(
                builder.connectTimeout, builder.reconnectInterval, builder.forceReconnectViaRoutingServers,
                TimeUnit.MILLISECONDS
        );
        this.processorInfoUpdateFrequency = builder.processorInfoUpdateFrequency;
        this.commandPermits = builder.commandPermits;
        this.queryPermits = builder.queryPermits;
    }

    /**
     * Returns a builder to configure a ConnectionFactory instance for the given {@code componentName}.
     * <p>
     * A unique clientInstanceId will be generated for this component. The componentName is used in monitoring
     * information and should be the same only for instances of the same application or component.
     *
     * @param componentName The name of the component connecting to AxonServer
     *
     * @return a builder instance for further configuration of the connector
     * @see #forClient(String, String)
     */
    public static Builder forClient(String componentName) {
        return forClient(componentName, componentName + "_" + randomHex(8));
    }


    /**
     * Returns a builder to configure a ConnectionFactory instance for the given {@code componentName} and {@code
     * clientInstanceId}.
     * <p>
     * The clientInstanceId MUST be a unique value across all instances that connect to AxonServer. The componentName is
     * used in monitoring information and should be the same only for instances of the same application or component.
     * <p>
     * Where possible, it is preferable that the {@code clientInstanceId} remains the same across restarts of a
     * component instance.
     *
     * @param componentName    The name of the component connecting to AxonServer
     * @param clientInstanceId The unique instance identifier for this instance of the component
     *
     * @return a builder instance for further configuration of the connector
     * @see #forClient(String)
     */
    public static Builder forClient(String componentName, String clientInstanceId) {
        return new AxonServerConnectionFactory.Builder(componentName, clientInstanceId);
    }

    /**
     * Connects to the given {@code context} using the settings defined in this ConnectionFactory.
     *
     * @param context The name of the context to connect to
     *
     * @return a Connection allowing interaction with the mentioned context
     */
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
                                    .setComponentName(componentName)
                                    .putAllTags(tags)
                                    .setVersion(CONNECTOR_VERSION)
                                    .build();

        return new ContextConnection(
                clientIdentification,
                executorService,
                new AxonServerManagedChannel(
                        routingServers,
                        reconnectConfiguration, context,
                        clientIdentification,
                        executorService,
                        this::createChannel
                ),
                processorInfoUpdateFrequency,
                commandPermits,
                queryPermits,
                context,
                cnx -> connections.remove(context, cnx)
        );
    }

    private ManagedChannel createChannel(ServerAddress address, String context) {
        ManagedChannelBuilder<?> builder = connectionConfig.apply(
                NettyChannelBuilder.forAddress(address.getHostName(), address.getGrpcPort())
        );

        if (!suppressDownloadMessage) {
            builder.intercept(new DownloadInstructionInterceptor(System.out));
        }

        return builder.intercept(
                new GrpcBufferingInterceptor(50),
                new HeaderAttachingInterceptor<>(Headers.CONTEXT, context),
                new HeaderAttachingInterceptor<>(Headers.ACCESS_TOKEN, token)
        ).build();
    }

    /**
     * Shuts down the connector, disconnecting from all contexts. To reconnect after calling this method, a new instance
     * of the ConnectionFactory needs to be created.
     * <p>
     * In case where regular reconnection is required, consider using {@link AxonServerConnection#disconnect()} on the
     * connections created by this factory instead.
     *
     * @see AxonServerConnection#disconnect()
     */
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

    /**
     * Builder for AxonServerConnectionFactory instances. The methods on this class allow for configuration of the
     * {@link AxonServerConnectionFactory} instance used to connect to an AxonServer (cluster).
     * <p>
     * This class is not intended to be instantiated directly, but rather through {@link
     * AxonServerConnectionFactory#forClient(String)} or {@link AxonServerConnectionFactory#forClient(String, String)}.
     */
    public static class Builder {

        private final String componentName;
        private final String clientInstanceId;
        private final Map<String, String> tags = new HashMap<>();
        private int queryPermits = 5000;
        private int commandPermits = 5000;
        private long processorInfoUpdateFrequency = 2000;
        private List<ServerAddress> routingServers;
        private long connectTimeout = 10000;
        private String token;
        private boolean suppressDownloadMessage = false;
        private ScheduledExecutorService executorService;
        private Function<NettyChannelBuilder, NettyChannelBuilder> sslConfig = NettyChannelBuilder::usePlaintext;
        private Function<NettyChannelBuilder, NettyChannelBuilder> keepAliveConfig = UnaryOperator.identity();
        private Function<ManagedChannelBuilder<?>, ManagedChannelBuilder<?>> otherConfig = UnaryOperator.identity();
        private boolean forceReconnectViaRoutingServers = true;
        private long reconnectInterval = 2000;
        private int executorPoolSize = 2;

        /**
         * Initializes a builder with the mandatory parameters: the {@code componentName} and {@code clientInstanceId}.
         *
         * @param componentName    The name of the component connecting to AxonServer
         * @param clientInstanceId The unique instance identifier for this instance of the component
         */
        protected Builder(String componentName, String clientInstanceId) {
            this.componentName = componentName;
            this.clientInstanceId = clientInstanceId;
        }

        /**
         * Provides the addresses of the AxonServer instances used to set up the initial connection. Ideally, these are
         * all the known <em>admin</em> nodes of the AxonServer Cluster to connect with.
         * <p>
         * The addresses are tried in the order provided, until one send a successful reply.
         * <p>
         * Defaults to "localhost:8024".
         *
         * @param serverAddresses The addresses to try to set up the initial connection with.
         *
         * @return this builder for further configuration
         */
        public Builder routingServers(ServerAddress... serverAddresses) {
            suppressDownloadMessage = true;
            this.routingServers = new ArrayList<>(Arrays.asList(serverAddresses));
            return this;
        }

        /**
         * Sets the amount of time to wait in between attempts to connect to AxonServer. A single attempt involves
         * connecting to each of the configured routing Servers (see {@link #routingServers(ServerAddress...)}).
         * <p>
         * Defaults to 2 seconds.
         *
         * @param interval The amount of time to wait in between connection attempts
         * @param timeUnit The unit in which the interval is expressed
         *
         * @return this builder for further configuration
         */
        public Builder reconnectInterval(long interval, TimeUnit timeUnit) {
            this.reconnectInterval = timeUnit.toMillis(interval);
            return this;
        }

        /**
         * Sets the timeout within which a connection has to be established with an AxonServer instance.
         * <p>
         * Defaults to 10 seconds.
         *
         * @param timeout  The amount of time to wait for a connection to be established
         * @param timeUnit The unit in which the timout is expressed
         *
         * @return this builder for further configuration
         */
        public Builder connectTimeout(long timeout, TimeUnit timeUnit) {
            this.connectTimeout = timeUnit.toMillis(timeout);
            return this;
        }

        /**
         * Configures the given {@code additionalClientTags} which describe this client. Any existing tags with the same
         * key as any of the additional tags provided, will be overwritten.
         * <p>
         * Tags are used to locate the best matching AxonServer instance to connect with. The instance with the most
         * matching Tags (both key and value must be equal) will be selected.
         * <p>
         * By default, no tags are defined.
         *
         * @param additionalClientTags additional tags that define this client component
         *
         * @return this builder for further configuration
         */
        public Builder clientTags(Map<String, String> additionalClientTags) {
            this.tags.putAll(additionalClientTags);
            return this;
        }

        /**
         * Configures an additional Tag with given {@code key} and {@code value} which describes this client. Any
         * existing tags with the same key will be overwritten.
         * <p>
         * Tags are used to locate the best matching AxonServer instance to connect with. The instance with the most
         * matching Tags (both key and value must be equal) will be selected.
         * <p>
         * By default, no tags are defined.
         *
         * @param key   the key of the Tag to configure
         * @param value the value of the Tag to configure
         *
         * @return this builder for further configuration
         */
        public Builder clientTag(String key, String value) {
            this.tags.put(key, value);
            return this;
        }

        /**
         * Defines the token used to authorize activity from this component on AxonServer.
         * <p>
         * Defaults to not using a Token. Defining a Token is mandatory when token-based authentication is enabled on
         * AxonServer.
         *
         * @param token The token to which the required authorizations have been assigned.
         *
         * @return this builder for further configuration
         */
        public Builder token(String token) {
            this.token = token;
            return this;
        }

        /**
         * Configures the use of Transport Layer Security (TLS) using the default settings from the JVM.
         * <p>
         * Defaults to not using TLS.
         *
         * @return this builder for further configuration
         * @see #useTransportSecurity(SslContext)
         */
        public Builder useTransportSecurity() {
            sslConfig = NettyChannelBuilder::useTransportSecurity;
            return this;
        }

        /**
         * Configures the use of Transport Layer Security (TLS) using the settings from the given {@code sslContext}.
         * <p>
         * Defaults to not using TLS.
         *
         * @param sslContext The context defining TLS parameters
         *
         * @return this builder for further configuration
         * @see SslContextBuilder#forClient()
         */
        public Builder useTransportSecurity(SslContext sslContext) {
            sslConfig = cb -> cb.sslContext(sslContext);
            return this;
        }

        /**
         * Indicates whether the connector should always reconnect via the Routing Servers. When {@code true}, the
         * connector will contact the Routing Servers for a new destination each time a connection is dropped. When
         * {@code false} (default), the connector will first attempt to re-establish a connection to the node is was
         * previously connected to. When that fails, only then will it contact the Routing Servers.
         * <p>
         * Default to {@code false}, causing the connector to first reattempt connecting to the previously connected
         * AxonServer instance.
         *
         * @param forceReconnectViaRoutingServers whether to force a reconnect to the Cluster via the RoutingServers.
         *
         * @return this builder for further configuration
         */
        public Builder forceReconnectViaRoutingServers(boolean forceReconnectViaRoutingServers) {
            this.forceReconnectViaRoutingServers = forceReconnectViaRoutingServers;
            return this;
        }

        /**
         * Defines the number of Threads that should be used for Connection Management activities by this connector.
         * This includes activities related to connecting to AxonServer, setting up instruction streams, sending and
         * validating heartbeats, etc.
         * <p>
         * Defaults to 2.
         *
         * @param poolSize The number of threads to assign to Connection related activities.
         *
         * @return this builder for further configuration
         */
        public Builder threadPoolSize(int poolSize) {
            this.executorPoolSize = poolSize;
            return this;
        }

        /**
         * Sets the time without read activity before sending a keepalive ping. An unreasonably small value might be
         * increased, and Long.MAX_VALUE nano seconds or an unreasonably large value will disable keepalive. Defaults to
         * infinite.
         * <p>
         * If no read activity is recorded within the given timeout after sending a keepalive ping, the connection is
         * considered dead.
         * <p>
         * Clients must receive permission from the service owner before enabling this option. Keepalives can increase
         * the load on services and are commonly "invisible" making it hard to notice when they are causing excessive
         * load. Clients are strongly encouraged to use only as small of a value as necessary.
         *
         * @param interval time without read activity before sending a keepalive ping
         * @param timeout  the time waiting for read activity after sending a keepalive ping
         * @param timeUnit the unit in which the interval and timeout are expressed
         *
         * @return this builder for further configuration
         */
        public Builder usingKeepAlive(long interval, long timeout, TimeUnit timeUnit, boolean keepAliveWithoutCalls) {
            keepAliveConfig = cb -> cb.keepAliveTime(interval, timeUnit)
                                      .keepAliveTimeout(timeout, timeUnit)
                                      .keepAliveWithoutCalls(keepAliveWithoutCalls);
            return this;
        }

        /**
         * Sets the maximum size for inbound messages. Requests receiving messages larger than the threshold will be
         * cancelled.
         * <p>
         * Default to 4 MiB.
         *
         * @param bytes The number of bytes to limit inbound message to
         *
         * @return this builder for further configuration
         */
        public Builder maxInboundMessageSize(int bytes) {
            otherConfig = otherConfig.andThen(cb -> cb.maxInboundMessageSize(bytes));
            return this;
        }

        /**
         * Registers the given {@code customization}, which configures the underling {@link ManagedChannelBuilder} used
         * to set up connections to AxonServer.
         * <p>
         * This method may be used in case none of the operations on this Builder provide support for the required
         * feature.
         *
         * @param customization A function defining the customization to make on the ManagedChannelBuilder
         *
         * @return this builder for further configuration
         */
        public Builder customize(UnaryOperator<ManagedChannelBuilder<?>> customization) {
            otherConfig = otherConfig.andThen(customization);
            return this;
        }

        /**
         * Sets the frequency in which the status of all Event Processors is emitted to the Server. Defaults to 2
         * seconds.
         *
         * @param interval The interval in which to send status updates
         * @param unit     The unit of time in which the interval is expressed
         *
         * @return this builder for further configuration
         */
        public Builder processorInfoUpdateFrequency(long interval, TimeUnit unit) {
            this.processorInfoUpdateFrequency = unit.toMillis(interval);
            return this;
        }

        /**
         * Sets the number of messages that a Query Handler may receive before any of them have been processed. Defaults
         * to 5000.
         * <p>
         * Values lower than 16 will be replaced with 16.
         *
         * @param permits The number of initial permits
         *
         * @return this builder for further configuration
         */
        public Builder queryPermits(int permits) {
            this.queryPermits = Math.max(16, permits);
            return this;
        }

        /**
         * Sets the number of messages that a Command Handler may receive before any of them have been processed.
         * Defaults to 5000.
         * <p>
         * Values lower than 16 will be replaced with 16.
         *
         * @param permits The number of initial permits
         *
         * @return this builder for further configuration
         */
        public Builder commandPermits(int permits) {
            this.commandPermits = Math.max(16, permits);
            return this;
        }

        /**
         * Validates the state of the builder, setting defaults where necessary.
         * <p>
         * This method is protected to allow overriding by subclasses. Subclasses are recommended to call this method as
         * part of their own validation process.
         */
        protected void validate() {
            if (routingServers == null) {
                routingServers = Collections.singletonList(new ServerAddress());
            }
            if (executorService == null) {
                executorService = new ScheduledThreadPoolExecutor(
                        executorPoolSize, AxonConnectorThreadFactory.forInstanceId(clientInstanceId)
                );
            }
        }

        /**
         * Builds an {@link AxonServerConnectionFactory} using the setting defined in this builder instance.
         *
         * @return a fully configured AxonServerConnectionFactory, ready to set up connections to AxonServer
         */
        public AxonServerConnectionFactory build() {
            validate();
            return new AxonServerConnectionFactory(this);
        }
    }

    private static class DownloadInstructionInterceptor implements ClientInterceptor {

        private final OutputStream out;
        private volatile boolean suppressDownloadMessage = false;

        public DownloadInstructionInterceptor(OutputStream out) {
            this.out = out;
        }

        @Override
        public <REQ, RESP> ClientCall<REQ, RESP> interceptCall(MethodDescriptor<REQ, RESP> method,
                                                               CallOptions callOptions, Channel next) {
            if (!suppressDownloadMessage && "io.axoniq.axonserver.grpc.control.PlatformService/GetPlatformServer"
                    .equals(method.getFullMethodName())) {
                return new ForwardingClientCall.SimpleForwardingClientCall<REQ, RESP>(
                        next.newCall(method, callOptions)
                ) {
                    @Override
                    public void start(Listener<RESP> responseListener, Metadata headers) {
                        super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RESP>(
                                responseListener
                        ) {
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
                        out.write(buffer, 0, read);
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
