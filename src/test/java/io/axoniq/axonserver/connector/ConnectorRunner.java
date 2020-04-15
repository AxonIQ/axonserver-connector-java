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

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;

import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectorRunner {

    private static CompletableFuture<CommandResponse> handle(Command command) {
        return CompletableFuture.completedFuture(
                CommandResponse.newBuilder()
                               .setMessageIdentifier(UUID.randomUUID().toString())
                               .setRequestIdentifier(command.getMessageIdentifier())
                               .setPayload(command.getPayload()).build());
    }

    public static class Sender {

        public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient");
            AxonServerConnection contextConnection = testSubject
                    .connect("default");

            CommandChannel channel = contextConnection.commandChannel();
            try {
                long started = System.currentTimeMillis();
                AtomicReference<CompletableFuture<?>> results = new AtomicReference<>(CompletableFuture.completedFuture(null));
                for (int c = 0; c < 1_000_000; c++) {
                    CompletableFuture<CommandResponse> result = channel.sendCommand(Command.newBuilder()
                                                                                           .setPayload(SerializedObject.newBuilder()
                                                                                                                       .setType("String")
                                                                                                                       .setData(ByteString.copyFromUtf8("Hello " + (c + 1) + " of 1000"))
                                                                                                                       .build())
                                                                                           .setMessageIdentifier(UUID.randomUUID().toString())
                                                                                           .setName("test")
                                                                                           .build());
                    results.accumulateAndGet(result, (r1, r2) -> CompletableFuture.allOf(r1, r2));
                }
                long dispatchingCompleted = System.currentTimeMillis();
                System.out.println("Dispatching took " + (dispatchingCompleted - started) + "ms");
                try {
                    results.get().get(5, TimeUnit.MINUTES);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    e.printStackTrace();
                }
                long resultsCompleted = System.currentTimeMillis();
                System.out.println("Full handling took " + (resultsCompleted - started) + "ms");
            } finally {
                contextConnection.disconnect();
            }

        }

    }

    public static class Handler {
        public static void main(String[] args) {
            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient");
            AxonServerConnection contextConnection = testSubject
                    .connect("default");

            InstructionChannel instructionChannel = contextConnection.instructionChannel();
            CommandChannel commandChannel = contextConnection.commandChannel();

            commandChannel.registerCommandHandler(ConnectorRunner::handle, "test");

            new Scanner(System.in).nextLine();

            testSubject.shutdown();
        }
    }
}