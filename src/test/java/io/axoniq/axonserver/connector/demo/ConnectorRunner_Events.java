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

package io.axoniq.axonserver.connector.demo;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.event.AggregateEventStream;
import io.axoniq.axonserver.connector.event.AppendEventsTransaction;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ConnectorRunner_Events {

    public static class EventHandler {

        public static void main(String[] args) throws InterruptedException {


            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient")
                                                                                 .build();
            AxonServerConnection contextConnection = testSubject.connect("default");

            EventChannel channel = contextConnection.eventChannel();
            ResultStream<EventWithToken> stream = channel.openStream(-1, 10000);
            long counter = 0;
            long t1 = System.currentTimeMillis();
            while (stream.nextIfAvailable(1, TimeUnit.SECONDS) != null) {
                counter++;
                long now = System.currentTimeMillis();
                if (now - t1 > 1000) {
                    System.out.println("Processed " + counter + " events in last second");
                    counter = 0;
                    t1 = now;
                }
            }
            stream.close();
        }


    }

    public static class ReadAggregateStream {

        public static void main(String[] args) throws Exception {
            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient")
                                                                                 .build();
            AxonServerConnection contextConnection = testSubject
                    .connect("default");

            EventChannel channel = contextConnection.eventChannel();
            AggregateEventStream result = channel.loadSnapshots("571a3aeb-39de-49bb-852f-8ce5f3acc4ed", Long.MAX_VALUE, 10);
            while (result.hasNext()) {
                System.out.println("Got event seq " + result.next().getAggregateSequenceNumber());
            }
            System.out.println("Done.");
        }
    }

    public static class Tokens {

        public static void main(String[] args) throws Exception {
            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient")
                                                                                 .build();
            AxonServerConnection contextConnection = testSubject
                    .connect("default");

            EventChannel channel = contextConnection.eventChannel();
            CompletableFuture<?> done = CompletableFuture.allOf(
                    channel.getFirstToken().whenComplete((r, e) -> System.out.println("First token:" + r)),
                    channel.getLastToken().whenComplete((r, e) -> System.out.println("Last token:" + r)),
                    channel.getTokenAt(Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli()).whenComplete((r, e) -> System.out.println("Token an hour ago :" + r)));

            done.get();
        }
    }

    public static class EventSender {

        public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient")
                                                                                 .build();
            AxonServerConnection contextConnection = testSubject
                    .connect("default");

            EventChannel channel = contextConnection.eventChannel();
            try {
                long started = System.currentTimeMillis();
                CompletableFuture<?> result = CompletableFuture.completedFuture(null);
                String aggregateIdentifier = UUID.randomUUID().toString();
                System.out.println("Generated dataset for " + aggregateIdentifier);
                for (int c = 0; c < 2000; c++) {
                    AppendEventsTransaction tx = channel.startAppendEventsTransaction();
//                    for (int i = 0; i < 10; i++) {
                    if (c != 0 && c % 50 == 0) {
                        CompletableFuture<?> snapshot = channel.appendSnapshot(buildEvent(aggregateIdentifier, c));
                        snapshot.get();
                    }
                        tx.appendEvent(buildEvent(aggregateIdentifier, c));
//                    }
                    tx.commit().get();

                }
                result.get(15, TimeUnit.SECONDS);
                long dispatchingCompleted = System.currentTimeMillis();
                System.out.println("Dispatching took " + (dispatchingCompleted - started) + "ms");
            } finally {
                contextConnection.disconnect();
                testSubject.shutdown();
            }

        }

        private static Event buildEvent(String aggregateIdentifier, long sequence) {
            return Event.newBuilder()
                        .setPayload(SerializedObject.newBuilder()
                                                    .setType(String.class.getName())
                                                    .setData(ByteString.copyFromUtf8("test")).build())
                        .setTimestamp(System.currentTimeMillis())
                        .setAggregateType("Test")
                        .setAggregateIdentifier(aggregateIdentifier)
                        .setAggregateSequenceNumber(sequence)
                        .setMessageIdentifier(UUID.randomUUID().toString())
                        .build();
        }

    }
}