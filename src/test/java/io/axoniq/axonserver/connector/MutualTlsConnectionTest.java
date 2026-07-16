/*
 * Copyright (c) 2026. AxonIQ
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

import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformInfo;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformServiceGrpc;
import io.grpc.Server;
import io.grpc.TlsServerCredentials;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class validating the (mutual) TLS configuration options of the {@link AxonServerConnectionFactory}, against a
 * stub PlatformService endpoint secured with TLS.
 *
 * @author Allard Buijze
 */
class MutualTlsConnectionTest {

    private Server server;
    private int serverPort;
    private AxonServerConnectionFactory connectionFactory;

    @AfterEach
    void tearDown() {
        if (connectionFactory != null) {
            connectionFactory.shutdown();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void connectsWithClientCertificateWhenServerRequiresClientAuth() throws Exception {
        startServer(TlsServerCredentials.ClientAuth.REQUIRE);

        AxonServerConnection connection = connect(
                factory -> factory.useMutualTransportSecurity(tlsFile("client.crt"),
                                                              tlsFile("client.key"),
                                                              tlsFile("ca.crt"))
        );

        assertWithin(10, TimeUnit.SECONDS, () -> assertTrue(connection.isConnected()));
    }

    @Test
    void failsToConnectWithoutClientCertificateWhenServerRequiresClientAuth() throws Exception {
        startServer(TlsServerCredentials.ClientAuth.REQUIRE);

        AxonServerConnection connection = connect(
                factory -> factory.useTransportSecurity(tlsFile("ca.crt"))
        );

        assertWithin(10, TimeUnit.SECONDS, () -> assertTrue(connection.isConnectionFailed()));
        assertFalse(connection.isConnected());
    }

    @Test
    void connectsWithTrustedCertificatesOnlyWhenServerDoesNotRequireClientAuth() throws Exception {
        startServer(TlsServerCredentials.ClientAuth.NONE);

        AxonServerConnection connection = connect(
                factory -> factory.useTransportSecurity(tlsFile("ca.crt"))
        );

        assertWithin(10, TimeUnit.SECONDS, () -> assertTrue(connection.isConnected()));
    }

    @Test
    void mutualTransportSecurityRejectsMissingCertificateOrKey() {
        AxonServerConnectionFactory.Builder builder = AxonServerConnectionFactory.forClient("mtls-test");

        assertThrows(NullPointerException.class,
                     () -> builder.useMutualTransportSecurity(null, tlsFile("client.key")));
        assertThrows(NullPointerException.class,
                     () -> builder.useMutualTransportSecurity(tlsFile("client.crt"), null));
        assertThrows(IllegalArgumentException.class,
                     () -> builder.useMutualTransportSecurity(new File("does-not-exist.crt"),
                                                              new File("does-not-exist.key")));
    }

    private void startServer(TlsServerCredentials.ClientAuth clientAuth) throws Exception {
        TlsServerCredentials.Builder credentials = TlsServerCredentials.newBuilder()
                                                                       .keyManager(tlsFile("server.crt"),
                                                                                   tlsFile("server.key"))
                                                                       .clientAuth(clientAuth);
        if (clientAuth != TlsServerCredentials.ClientAuth.NONE) {
            credentials.trustManager(tlsFile("ca.crt"));
        }
        server = NettyServerBuilder.forPort(freePort(), credentials.build())
                                   .addService(new StubPlatformService())
                                   .build()
                                   .start();
    }

    private AxonServerConnection connect(UnaryOperator<AxonServerConnectionFactory.Builder> tlsConfig) {
        connectionFactory = tlsConfig.apply(AxonServerConnectionFactory.forClient("mtls-test")
                                                                       .routingServers(new ServerAddress("localhost",
                                                                                                         serverPort)))
                                     .build();
        return connectionFactory.connect("default");
    }

    private int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            serverPort = socket.getLocalPort();
            return serverPort;
        }
    }

    private File tlsFile(String name) {
        try {
            return new File(getClass().getResource("/tls/" + name).toURI());
        } catch (Exception e) {
            throw new IllegalStateException("Could not locate test resource /tls/" + name, e);
        }
    }

    private static class StubPlatformService extends PlatformServiceGrpc.PlatformServiceImplBase {

        @Override
        public void getPlatformServer(ClientIdentification request, StreamObserver<PlatformInfo> responseObserver) {
            responseObserver.onNext(PlatformInfo.newBuilder().setSameConnection(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<PlatformInboundInstruction> openStream(
                StreamObserver<PlatformOutboundInstruction> responseObserver) {
            return new StreamObserver<PlatformInboundInstruction>() {
                @Override
                public void onNext(PlatformInboundInstruction value) {
                    // no-op: the stub only needs to keep the stream open
                }

                @Override
                public void onError(Throwable t) {
                    // no-op
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
