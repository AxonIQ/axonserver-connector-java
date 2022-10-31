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

import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Communication channel with AxonServer for Command related interactions.
 */
public interface CommandChannel {

    /**
     * Registers the given {@code handler} to handle incoming commands with given {@code commandNames}, using the given
     * {@code loadFactor}. If handlers had already been registered for any of the given command names, this registration
     * replaces the existing one for those command names. Other commands remain unaffected.
     *
     * @param handler      the handler to handle incoming commands with
     * @param loadFactor   the relative load factor for this handler
     * @param commandNames the names of the commands to register the handler for
     * @return a registration which allows the command handler to be deregistered
     */
    Registration registerCommandHandler(Function<Command, CompletableFuture<CommandResponse>> handler,
                                        int loadFactor,
                                        String... commandNames);

    /**
     * Sends the give Command to AxonServer for routing to an appropriate handler.
     *
     * @param command the command to send
     * @return a CompletableFuture providing the result of command execution
     */
    CompletableFuture<CommandResponse> sendCommand(Command command);

    /**
     * Prepares this {@link CommandChannel} to disconnect, by unsubscribing all registered command handlers. Will wait
     * with a certain cut off until all acknowledgments of unsubscribing have been received.
     * <p>
     * This method should be used if a connected client wants to disconnect from AxonServer.
     *
     * @return a {@link CompletableFuture} of {@link Void} to react when all command handlers have been unsubscribed
     */
    CompletableFuture<Void> prepareDisconnect();
}
