/*
 * Copyright (c) 2020-2023. AxonIQ
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

package io.axoniq.axonserver.connector.event.transformation;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class ETChannelTest {

    private final int count = 400;
    private AxonServerConnection connection;
    private AxonServerConnectionFactory client;

    @BeforeEach
    void setUp() {
        client = AxonServerConnectionFactory.forClient("event-transformer")
                                            .routingServers(new ServerAddress("localhost", 8124))
                                            .reconnectInterval(500, MILLISECONDS)
                                            .build();
        connection = client.connect("default");
    }

    @AfterEach
    void tearDown() {
        connection.disconnect();
        client.shutdown();
    }

    @Test
    void appendEvents() throws ExecutionException, InterruptedException {
        CompletableFuture completableFuture = new CompletableFuture<>();
        for (int i = 0; i < count; i++) {
            completableFuture = connection.eventChannel().appendEvents(event("event " + i));
        }
        completableFuture.get();
    }

    @Test
    void createNewTransaction() throws ExecutionException, InterruptedException {
        connection.eventTransformationChannel()
                  .newTransformation("new transformation")
                  .get();
    }

    @Test
    void deleteAll() throws ExecutionException, InterruptedException {
        connection.eventTransformationChannel()
                  .transform("Transformation test", transformer -> {
                      for (int i = 0; i < count; i++) {
                          transformer.deleteEvent(i);
                      }
                  }).get();
    }

    @Test
    void replaceAll() throws ExecutionException, InterruptedException {
        connection.eventTransformationChannel()
                  .newTransformation("Replacement 1")
                  .thenCompose(activeTransformation -> activeTransformation.transform(
                          transformer -> {
                              for (int i = 0; i < count; i++) {
                                  transformer.replaceEvent(i, event("Replacement 1 of event " + i));
                              }
                          })).thenCompose(ActiveTransformation::startApplying)
                  .get();
    }

    @Test
    void cancel() throws ExecutionException, InterruptedException {
        connection.eventTransformationChannel()
                  .activeTransformation()
                  .thenCompose(ActiveTransformation::cancel)
                  .get();
    }


    private Event event(String payload) {
        return Event.newBuilder()
                    .setPayload(
                            SerializedObject.newBuilder()
                                            .setData(ByteString.copyFromUtf8(payload)))
                    .build();
    }
}
