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

import io.axoniq.axonserver.connector.impl.ContextAddingInterceptor;
import io.axoniq.axonserver.connector.impl.ContextConnection;
import io.axoniq.axonserver.connector.impl.GrpcBufferingInterceptor;
import io.axoniq.axonserver.connector.impl.TokenAddingInterceptor;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.PlatformInfo;
import io.axoniq.axonserver.grpc.control.PlatformServiceGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;

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
    private boolean sslEnabled = false;
    private Iterable<? extends ServerAddress> routingServers;
    private long connectTimeout = 10000;
    private String token;
    private volatile boolean shutdown;
    private volatile boolean suppressDownloadMessage = false;
    private ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(5);

    protected AxonServerConnectionFactory(String applicationName, String clientInstanceId) {
        this.applicationName = applicationName;
        this.clientInstanceId = clientInstanceId;
        routingServers = singletonList(new ServerAddress());
    }

    public static AxonServerConnectionFactory forClient(String applicationName) {
        return new AxonServerConnectionFactory(applicationName, UUID.randomUUID().toString());
    }

    public static AxonServerConnectionFactory forClient(String applicationName, String clientInstanceId) {
        return new AxonServerConnectionFactory(applicationName, clientInstanceId);
    }

    public AxonServerConnectionFactory clientTags(Map<String, String> additionalTags) {
        this.tags.putAll(additionalTags);
        return this;
    }

    public AxonServerConnectionFactory clientTag(String key, String value) {
        this.tags.put(key, value);
        return this;
    }

    public AxonServerConnectionFactory token(String token) {
        this.token = token;
        return this;
    }

    public AxonServerConnection connect(String context) {
        ContextConnection c;
        return connections.computeIfAbsent(context, this::createConnection);
    }

    private ContextConnection createConnection(String context) {
        logger.info("Connecting using {}...",
                    sslEnabled ? "TLS" : "unencrypted connection");

        ClientIdentification clientIdentification =
                ClientIdentification.newBuilder()
                                    .setClientId(clientInstanceId)
                                    .setComponentName(applicationName)
                                    .putAllTags(tags)
                                    .setVersion(CONNECTOR_VERSION)
                                    .build();

        return new ContextConnection(context, clientIdentification, executorService, this::openChannel);
    }

    private ManagedChannel openChannel(String context, ClientIdentification clientIdentification) throws AxonServerException {
        ManagedChannel connection = null;
        for (ServerAddress nodeInfo : routingServers) {
            ManagedChannel candidate = createChannel(nodeInfo.hostName(), nodeInfo.grpcPort(), context);
            PlatformServiceGrpc.PlatformServiceBlockingStub stub =
                    PlatformServiceGrpc.newBlockingStub(candidate)
                                       .withDeadlineAfter(connectTimeout, TimeUnit.MILLISECONDS);
            try {
                logger.info("Requesting connection details from {}:{}",
                            nodeInfo.hostName(), nodeInfo.grpcPort());
                PlatformInfo clusterInfo = stub.getPlatformServer(clientIdentification);
                logger.debug("Received PlatformInfo suggesting [{}] ({}:{}), {}",
                             clusterInfo.getPrimary().getNodeName(),
                             clusterInfo.getPrimary().getHostName(),
                             clusterInfo.getPrimary().getGrpcPort(),
                             clusterInfo.getSameConnection() ? "reusing existing connection" : "using new connection");
                if (clusterInfo.getSameConnection()
                        || (clusterInfo.getPrimary().getGrpcPort() == nodeInfo.grpcPort()
                        && clusterInfo.getPrimary().getHostName().equals(nodeInfo.hostName()))) {
                    logger.info("Reusing existing channel");
                    connection = candidate;
                } else {
                    shutdownNow(candidate);
                    logger.info("Connecting to [{}] ({}:{})",
                                clusterInfo.getPrimary().getNodeName(),
                                clusterInfo.getPrimary().getHostName(),
                                clusterInfo.getPrimary().getGrpcPort());
                    connection = createChannel(
                            clusterInfo.getPrimary().getHostName(),
                            clusterInfo.getPrimary().getGrpcPort(),
                            context
                    );
                }
                break;
            } catch (StatusRuntimeException sre) {
                shutdownNow(candidate);
                logger.warn(
                        "Connecting to AxonServer node [{}]:[{}] failed: {}",
                        nodeInfo.hostName(), nodeInfo.grpcPort, sre.getMessage()
                );
            }
        }

        if (connection == null) {
            if (!suppressDownloadMessage) {
                suppressDownloadMessage = true;
                writeDownloadMessage();
            }
            throw new AxonServerException(ErrorCode.CONNECTION_FAILED.errorCode(),
                                          "No connection to AxonServer available");
        } else {
            suppressDownloadMessage = true;
        }
        return connection;
    }

    private void writeDownloadMessage() {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("axonserver_download.txt")) {
            byte[] buffer = new byte[1024];
            int read;
            while (in != null && (read = in.read(buffer, 0, 1024)) >= 0) {
                System.out.write(buffer, 0, read);
            }
        } catch (IOException e) {
            logger.debug("Unable to write download advice. You're on your own now.", e);
        }
    }

    private void shutdownNow(ManagedChannel managedChannel) {
        try {
            managedChannel.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Interrupted during shutdown");
        }
    }

    private ManagedChannel createChannel(String hostName, int port, String context) {
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(hostName, port);

        // TODO - configure gRPC KeepAlive pings
//        if (axonServerConfiguration.getKeepAliveTime() > 0) {
//            builder.keepAliveTime(axonServerConfiguration.getKeepAliveTime(), TimeUnit.MILLISECONDS)
//                   .keepAliveTimeout(axonServerConfiguration.getKeepAliveTimeout(), TimeUnit.MILLISECONDS)
//                   .keepAliveWithoutCalls(true);
//        }

        // TODO - configure max inbound message size
//        if (axonServerConfiguration.getMaxMessageSize() > 0) {
//            builder.maxInboundMessageSize(axonServerConfiguration.getMaxMessageSize());
//        }

        // TODO - configure SSL
//        if (axonServerConfiguration.isSslEnabled()) {
//            try {
//                if (axonServerConfiguration.getCertFile() != null) {
//                    File certFile = new File(axonServerConfiguration.getCertFile());
//                    if (!certFile.exists()) {
//                        throw new RuntimeException(
//                                "Certificate file [" + axonServerConfiguration.getCertFile() + "] does not exist"
//                        );
//                    }
//                    SslContext sslContext = GrpcSslContexts.forClient()
//                                                           .trustManager(new File(
//                                                                   axonServerConfiguration.getCertFile()
//                                                           ))
//                                                           .build();
//                    builder.sslContext(sslContext);
//                }
//            } catch (SSLException e) {
//                throw new RuntimeException("Couldn't set up SSL context", e);
//            }
//        } else {
        builder.usePlaintext();
//        }

        return builder.intercept(new GrpcBufferingInterceptor(50),
                                 new ContextAddingInterceptor(context),
                                 new TokenAddingInterceptor(token)
        )
                      .build();
    }

    public void shutdown() {
        shutdown = true;
        connections.forEach((k, conn) -> conn.disconnect());
    }

    private static class ServerAddress {

        private final int grpcPort;
        private final String host;

        public ServerAddress() {
            this("localhost");
        }

        public ServerAddress(String host) {
            this(host, 8124);
        }

        public ServerAddress(String host, int grpcPort) {
            this.grpcPort = grpcPort;
            this.host = host;
        }

        public int grpcPort() {
            return grpcPort;
        }

        public String hostName() {
            return host;
        }
    }
}
