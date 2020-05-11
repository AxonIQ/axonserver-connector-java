package io.axoniq.axonserver.connector.command;

import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface CommandChannel {

    Registration registerCommandHandler(Function<Command, CompletableFuture<CommandResponse>> handler, String... commandNames);

    CompletableFuture<CommandResponse> sendCommand(Command command);
}
