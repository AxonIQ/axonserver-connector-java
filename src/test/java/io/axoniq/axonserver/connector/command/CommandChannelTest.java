/*
 * Copyright (c) 2020. AxonIQ
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

package io.axoniq.axonserver.connector.command;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.silently;
import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandChannelTest {

    private AxonServerConnectionFactory connectionFactory1;
    private AxonServerConnection connection1;

    @BeforeEach
    void setUp() {
        connectionFactory1 = AxonServerConnectionFactory.forClient(getClass().getSimpleName(),
                                                                   "client1")
                                                        .routingServers(new ServerAddress("127:0.0.1"))
                                                        .forceReconnectViaRoutingServers(false)
                                                        .reconnectInterval(500, TimeUnit.MILLISECONDS)
                                                        .build();
        connection1 = connectionFactory1.connect("default");
    }

    @AfterEach
    void tearDown() {
        silently(connectionFactory1, AxonServerConnectionFactory::shutdown);
    }

    @Test
    void testSubscribeWithMalformedUrl() throws InterruptedException {
        CommandChannel commandChannel = connection1.commandChannel();

        assertFalse(connection1.isConnected());
        assertWithin(1, TimeUnit.SECONDS, ()-> assertTrue(connection1.isConnectionFailed()));

        Registration result = commandChannel
                .registerCommandHandler(r -> CompletableFuture.completedFuture(CommandResponse.getDefaultInstance()), 100, "test");
        assertNotNull(result);
    }
}