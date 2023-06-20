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
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.connector.event.transformation.event.EventSources;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.junit.jupiter.api.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static io.axoniq.axonserver.connector.event.transformation.EventTransformation.State.APPLIED;
import static io.axoniq.axonserver.connector.event.transformation.EventTransformation.State.CANCELLED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class ETChannelTest extends AbstractAxonServerIntegrationTest {

    private static final int COUNT = 400;
    private static final String TRANSFORMED_EVENT_PREFIX = "Transformed event ";
    private static final String ORIGINAL_EVENT_PREFIX = "event ";
    private AxonServerConnection connection;
    private AxonServerConnectionFactory client;

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        client = AxonServerConnectionFactory.forClient("event-transformer")
                                            .connectTimeout(1500, TimeUnit.MILLISECONDS)
                                            .reconnectInterval(500, MILLISECONDS)
                                            .routingServers(axonServerAddress)
                                            .build();
        connection = client.connect("default");
        appendEvents();
    }

    @AfterEach
    void tearDown() {
        connection.disconnect();
        client.shutdown();
    }

    @Test
    void deleteAll() throws ExecutionException, InterruptedException {
        connection.eventTransformationChannel()
                  .transform("Transformation test", transformer -> {
                      for (int i = 0; i < COUNT; i++) {
                          transformer.deleteEvent(i);
                      }
                  }).get();
        waitForTransformationState(APPLIED);
        try (EventStream eventStream = connection.eventChannel()
                                                 .openStream(-1, 32)) {
            for (int i = 0; i < COUNT; i++) {
                assertEventDeleted(eventStream.next());
            }
        }
    }

    @Test
    void replaceAll() throws ExecutionException, InterruptedException {
        connection.eventTransformationChannel()
                  .newTransformation("Replacement 1")
                  .thenCompose(activeTransformation -> activeTransformation.transform(
                          transformer -> {
                              for (int i = 0; i < COUNT; i++) {
                                  transformer.replaceEvent(i, event(TRANSFORMED_EVENT_PREFIX + i));
                              }
                          })).thenCompose(ActiveTransformation::startApplying)
                  .get();
        waitForTransformationState(APPLIED);
        try (EventStream eventStream = connection.eventChannel()
                                                 .openStream(-1, 32)) {
            for (int i = 0; i < COUNT; i++) {
                assertEventTransformed(eventStream.next());
            }
        }
    }

    @Test
    void cancel() throws ExecutionException, InterruptedException {
        connection.eventTransformationChannel()
                  .newTransformation("Replacement 1")
                  .thenCompose(activeTransformation -> activeTransformation.transform(
                          transformer -> {
                              for (int i = 0; i < COUNT; i++) {
                                  transformer.replaceEvent(i, event(TRANSFORMED_EVENT_PREFIX + i));
                              }
                          }))
                  .get();
        connection.eventTransformationChannel()
                  .activeTransformation()
                  .thenCompose(ActiveTransformation::cancel)
                  .get();
        waitForTransformationState(CANCELLED);
        try (EventStream eventStream = connection.eventChannel()
                                                 .openStream(-1, 32)) {
            for (int i = 0; i < COUNT; i++) {
                assertEventNotTransformed(eventStream.next());
            }
        }
    }


    @Test
    void fluentApiTest() throws ExecutionException, InterruptedException {
        EventSources.range(() -> connection.eventChannel(), -1, COUNT - 1)
                    .filter(e -> e.getToken() % 2 == 0)
                    .transform("desc", (eventWithToken, appender) -> appender.deleteEvent(eventWithToken.getToken()))
                    .execute(() -> connection.eventTransformationChannel())
                    .get();
        waitForTransformationState(APPLIED);
        try (EventStream eventStream = connection.eventChannel()
                                                 .openStream(-1, 32)) {
            for (int i = 0; i < COUNT; i++) {
                EventWithToken event = eventStream.next();
                if (event.getToken() % 2 == 0) {
                    assertEventDeleted(event);
                } else {
                    assertEventNotTransformed(event);
                }
            }
        }
    }

    private void assertEventDeleted(EventWithToken event) {
        assertEquals("empty", event.getEvent()
                                   .getPayload()
                                   .getType());
    }

    private void assertEventNotTransformed(EventWithToken event) {
        assertEquals(ORIGINAL_EVENT_PREFIX + event.getToken(),
                     event.getEvent()
                          .getPayload()
                          .getData()
                          .toStringUtf8());
    }

    private void assertEventTransformed(EventWithToken event) {
        assertEquals(TRANSFORMED_EVENT_PREFIX + event.getToken(),
                     event.getEvent()
                          .getPayload()
                          .getData()
                          .toStringUtf8());
    }

    private void appendEvents() throws ExecutionException, InterruptedException {
        Event[] toAppend = IntStream.range(0, COUNT)
                                    .mapToObj(i -> event(ORIGINAL_EVENT_PREFIX + i))
                                    .toArray(Event[]::new);
        connection.eventChannel()
                  .appendEvents(toAppend)
                  .get();
    }

    private Event event(String payload) {
        return Event.newBuilder()
                    .setPayload(
                            SerializedObject.newBuilder()
                                            .setData(ByteString.copyFromUtf8(payload)))
                    .build();
    }

    private void waitForTransformationState(EventTransformation.State state)
            throws InterruptedException, ExecutionException {
        CountDownLatch latch = new CountDownLatch(1);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.submit(() -> stateCheck(state, latch, scheduler))
                 .get();
        assertTrue(latch.await(60, SECONDS));
        scheduler.shutdown();
    }

    private void stateCheck(EventTransformation.State state, CountDownLatch latch, ScheduledExecutorService scheduler) {
        try {
            if (state != connection.eventTransformationChannel()
                                   .transformations()
                                   .get()
                                   .iterator()
                                   .next()
                                   .state()) {
                scheduler.schedule(() -> stateCheck(state, latch, scheduler), 10, MILLISECONDS);
            } else {
                latch.countDown();
            }
        } catch (InterruptedException | ExecutionException e) {
            fail(e.getMessage());
        }
    }
}
