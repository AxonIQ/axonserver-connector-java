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
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectorRunner_Command {

    private static CompletableFuture<CommandResponse> handle(Command command) {
        return CompletableFuture.completedFuture(
                CommandResponse.newBuilder()
                               .setMessageIdentifier(UUID.randomUUID().toString())
                               .setRequestIdentifier(command.getMessageIdentifier())
                               .setPayload(command.getPayload()).build());
    }

    public static class CommandSender {

        public static void main(String[] args) {
            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient")
                                                                                 .build();
            AxonServerConnection contextConnection = testSubject
                    .connect("default");

            CommandChannel channel = contextConnection.commandChannel();
            try {
                long started = System.currentTimeMillis();
                AtomicReference<CompletableFuture<Integer>> results = new AtomicReference<>(CompletableFuture.completedFuture(0));
                for (int c = 0; c < 200_000; c++) {
                    CompletableFuture<Integer> result = channel.sendCommand(Command.newBuilder()
                                                                                   .setPayload(SerializedObject.newBuilder()
                                                                                                               .setType("String")
                                                                                                               .setData(ByteString.copyFromUtf8("Hello " + (c + 1) + " of 1000"))
                                                                                                               .build())
                                                                                .setMessageIdentifier(UUID.randomUUID().toString())
                                                                                .setName("test")
                                                                                .build())
                                                            .thenApply(CommandResponse::hasErrorMessage)
                            .thenApply(b -> b ? 1 : 0)
                            .exceptionally(e -> 1);
                    results.accumulateAndGet(result, (r1, r2) -> r1.thenCombine(r2, Integer::sum));
                }
                long dispatchingCompleted = System.currentTimeMillis();
                System.out.println("Dispatching took " + (dispatchingCompleted - started) + "ms");
                int errorCount = -1;
                try {
                    CompletableFuture<Integer> fullResult = results.get();
                    errorCount = fullResult.get(5, TimeUnit.MINUTES);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
//                    e.printStackTrace();
                }
                long resultsCompleted = System.currentTimeMillis();
                System.out.println("Full handling took " + (resultsCompleted - started) + "ms and reported " + errorCount + " errors." );
            } finally {
                contextConnection.disconnect();
                testSubject.shutdown();
            }

        }

    }

    public static class CommandHandler {
        private static final Logger logger = LoggerFactory.getLogger(CommandHandler.class);

        public static void main(String[] args) {
            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient")
                                                                                 .build();
            AxonServerConnection contextConnection = testSubject
                    .connect("default");

            CommandChannel commandChannel = contextConnection.commandChannel();

            AtomicInteger counter = new AtomicInteger();
            AtomicLong timer = new AtomicLong();
            try {
                commandChannel.registerCommandHandler(command -> {
                    if (counter.updateAndGet(t -> t == 9999 ? 0 : t + 1) == 0) {
                        long now = System.currentTimeMillis();
                        long previous = timer.getAndSet(now);
                        logger.info("Handled another 10000 in {} ms", now - previous);
                    }
                    return handle(command);
                }, 100, "test");

                new Scanner(System.in).nextLine();
            } finally {
                testSubject.shutdown();
            }
        }
    }
}