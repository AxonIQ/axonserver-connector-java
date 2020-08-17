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

package io.axoniq.axonserver.connector.demo;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.query.QueryChannel;
import io.axoniq.axonserver.connector.query.QueryDefinition;
import io.axoniq.axonserver.connector.query.SubscriptionQueryResult;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.ProcessingKey;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;

import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectorRunner_SubscriptionQuery {

    public static class QuerySender {

        public static void main(String[] args) throws Exception {
            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient")
                                                                                 .build();
            AxonServerConnection contextConnection = testSubject.connect("default");
            while (!contextConnection.isConnected()) {
                System.out.println("Waiting for connection...");
                Thread.sleep(1000);
            }
            try {
                QueryChannel channel = contextConnection.queryChannel();
                SubscriptionQueryResult result = channel.subscriptionQuery(QueryRequest.newBuilder()
                                                                                       .addProcessingInstructions(ProcessingInstruction.newBuilder().setKey(ProcessingKey.NR_OF_RESULTS).setValue(MetaDataValue.newBuilder().setNumberValue(20).build()).build())
                                                                                       .setQuery("java.lang.String")
                                                                                       .setPayload(SerializedObject.newBuilder().setType("java.lang.String").setData(ByteString.copyFromUtf8("Hello world")).build()).build(),
                                                                           SerializedObject.newBuilder().setType("java.lang.String").setData(ByteString.copyFromUtf8("Hello")).build(), 1000, 100);
                result.initialResult().whenComplete((r, e) -> System.out.println("Initial result: " + r.getPayload().getData().toStringUtf8()));
                ResultStream<QueryUpdate> updates = result.updates();
                long deadline = System.currentTimeMillis() + 15000;
                while (!updates.isClosed()) {
                    QueryUpdate response = updates.nextIfAvailable(5, TimeUnit.SECONDS);
                    if (response != null) {
                        System.out.println("Got response: " + response.getPayload().getData().toStringUtf8());
                    }
                    if (System.currentTimeMillis() > deadline) {
                        updates.close();
                    }
                }
                result.initialResult().get(5, TimeUnit.SECONDS);
            } finally {
                contextConnection.disconnect();
                testSubject.shutdown();
            }
        }
    }

    public static class QueryHandler {

        public static void main(String[] args) {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient-Success")
                                                                                 .build();
            AxonServerConnection contextConnection = testSubject.connect("default");

            QueryChannel channel = contextConnection.queryChannel();
            channel.registerQueryHandler(new io.axoniq.axonserver.connector.query.QueryHandler() {
                @Override
                public void handle(QueryRequest q, ReplyChannel<QueryResponse> r) {
                    System.out.println("Handled query");
                    r.sendLast(QueryResponse.newBuilder().setRequestIdentifier(q.getMessageIdentifier()).setPayload(q.getPayload()).build());
                }

                @Override
                public Registration registerSubscriptionQuery(SubscriptionQuery query, UpdateHandler updateHandler) {
                    System.out.println("Subscription arrived. Sending 5 messages and closing it....");
                    AtomicBoolean running = new AtomicBoolean(true);
                    Runnable sendIt = () -> {
                        if (running.get()) {
                            updateHandler.sendUpdate(QueryUpdate.newBuilder().setPayload(query.getQueryRequest().getPayload()).build());
                        }
                    };
                    ScheduledFuture<?> task = executor.scheduleWithFixedDelay(sendIt, 1, 1, TimeUnit.SECONDS);
                    return () -> {
                        running.set(false);
                        task.cancel(false);
                        System.out.println("Subscription query closed");
                        return CompletableFuture.completedFuture(null);
                    };
                }
            }, new QueryDefinition(String.class.getName(), String.class));

            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();

            contextConnection.disconnect();
            testSubject.shutdown();
        }
    }

    public static class FaultyQueryHandler {

        public static void main(String[] args) {
            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient-Faulty").build();
            AxonServerConnection contextConnection = testSubject.connect("default");

            QueryChannel channel = contextConnection.queryChannel();
            channel.registerQueryHandler((q, r) -> {
                                             System.out.println("Handled query");
                                             r.complete();
                                         }, new QueryDefinition(String.class.getName(), String.class),
                                         new QueryDefinition(String.class.getName(), Exception.class));

            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
        }
    }
}