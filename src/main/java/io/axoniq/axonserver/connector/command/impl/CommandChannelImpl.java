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

package io.axoniq.axonserver.connector.command.impl;

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.InstructionHandler;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AbstractIncomingInstructionStream;
import io.axoniq.axonserver.connector.impl.AsyncRegistration;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.ObjectUtils;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.ProcessingKey;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.command.CommandServiceGrpc;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.grpc.stub.StreamObserver;
import io.netty.util.internal.OutOfDirectMemoryError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;

/**
 * {@link CommandChannel} implementation, serving as the command connection between AxonServer and a client
 * application.
 */
public class CommandChannelImpl extends AbstractAxonServerChannel implements CommandChannel {

    private static final Logger logger = LoggerFactory.getLogger(CommandChannelImpl.class);

    private final AtomicReference<StreamObserver<CommandProviderOutbound>> outboundCommandStream = new AtomicReference<>();
    private final ClientIdentification clientIdentification;
    private final ConcurrentMap<String, Function<Command, CompletableFuture<CommandResponse>>> commandHandlers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CompletableFuture<Void>> instructionsAwaitingAck = new ConcurrentHashMap<>();
    private final ConcurrentMap<CommandProviderInbound.RequestCase, InstructionHandler<CommandProviderInbound, CommandProviderOutbound>> handlers = new ConcurrentHashMap<>();
    private final int permits;
    private final int permitsBatch;
    private final CommandServiceGrpc.CommandServiceStub commandServiceStub;

    /**
     * Constructs a {@link CommandChannelImpl}.
     *
     * @param clientIdentification the information identifying the client application which is connecting
     * @param permits              an {@code int} defining the number of permits this channel has
     * @param permitsBatch         an {@code int} defining the number of permits to be consumed from prior to requesting
     *                             additional permits for this channel
     * @param executor             a {@link ScheduledExecutorService} used to schedule reconnects of this channel
     * @param channel              the {@link AxonServerManagedChannel} used to form the connection with AxonServer
     */
    public CommandChannelImpl(ClientIdentification clientIdentification,
                              int permits,
                              int permitsBatch,
                              ScheduledExecutorService executor,
                              AxonServerManagedChannel channel) {
        super(executor, channel);
        this.clientIdentification = clientIdentification;
        this.permits = permits;
        this.permitsBatch = permitsBatch;
        this.handlers.put(CommandProviderInbound.RequestCase.COMMAND, this::handleIncomingCommand);
        this.handlers.put(CommandProviderInbound.RequestCase.ACK, this::handleAck);
        this.commandServiceStub = CommandServiceGrpc.newStub(channel);
    }

    private void handleIncomingCommand(CommandProviderInbound message, ReplyChannel<CommandProviderOutbound> outbound) {
        Command command = message.getCommand();
        Function<Command, CompletableFuture<CommandResponse>> handler = commandHandlers.get(command.getName());
        if (handler != null) {
            outbound.sendAck();
        } else {
            outbound.sendNack();
            handler = c -> noHandlerForCommand();
        }

        handler.apply(command)
               .exceptionally(e -> CommandResponse.newBuilder()
                                                  .setErrorCode(ErrorCategory.COMMAND_EXECUTION_ERROR.errorCode())
                                                  .setErrorMessage(
                                                          ErrorMessage.newBuilder()
                                                                      .setMessage(e.getMessage())
                                                                      .build()
                                                  )
                                                  .build())
               .thenApply(CommandResponse::newBuilder)
               .thenApply(r -> r.setRequestIdentifier(command.getMessageIdentifier()))
               .whenComplete((r, e) -> outbound.send(
                       CommandProviderOutbound.newBuilder().setCommandResponse(r).build()
               ))
               .thenRun(outbound::complete);
    }

    private CompletableFuture<CommandResponse> noHandlerForCommand() {
        CompletableFuture<CommandResponse> r = new CompletableFuture<>();
        r.completeExceptionally(new AxonServerException(ErrorCategory.NO_HANDLER_FOR_COMMAND,
                                                        "No handler for command",
                                                        clientIdentification.getClientId()));
        return r;
    }

    private void handleAck(CommandProviderInbound message, ReplyChannel<CommandProviderOutbound> outbound) {
        InstructionAck ack = message.getAck();
        CompletableFuture<Void> instructionResult = instructionsAwaitingAck.remove(ack.getInstructionId());
        if (instructionResult == null) {
            return;
        }
        if (ack.getSuccess()) {
            instructionResult.complete(null);
        } else {
            instructionResult.completeExceptionally(new AxonServerException(ack.getError()));
        }
    }

    @Override
    public synchronized void connect() {
        if (outboundCommandStream.get() != null) {
            // we're already connected on this channel
            return;
        }
        IncomingCommandStream responseObserver = new IncomingCommandStream(
                clientIdentification.getClientId(), permits, permitsBatch, this::onConnectionError
        );

        //noinspection ResultOfMethodCallIgnored
        commandServiceStub.openStream(responseObserver);

        StreamObserver<CommandProviderOutbound> newValue = responseObserver.getInstructionsForPlatform();

        StreamObserver<CommandProviderOutbound> previous = outboundCommandStream.getAndSet(newValue);

        commandHandlers.keySet().forEach(k -> newValue.onNext(buildSubscribeMessage(k, "", 100)));
        responseObserver.enableFlowControl();


        logger.info("CommandChannel connected, {} command handlers registered", commandHandlers.size());
        ObjectUtils.silently(previous, StreamObserver::onCompleted);
    }

    private void onConnectionError(Throwable error) {
        instructionsAwaitingAck.keySet().forEach(
                k -> doIfNotNull(instructionsAwaitingAck.remove(k), f -> f.completeExceptionally(error))
        );
        scheduleReconnect();
    }

    @Override
    public void disconnect() {
        commandHandlers.keySet().forEach(this::sendUnsubscribe);

        // TODO - Wait for all received commands to have returned their responses

        commandHandlers.clear();
        doIfNotNull(outboundCommandStream.getAndSet(null), StreamObserver::onCompleted);
    }

    @Override
    public boolean isConnected() {
        return outboundCommandStream.get() != null;
    }

    @Override
    public Registration registerCommandHandler(Function<Command, CompletableFuture<CommandResponse>> handler,
                                               int loadFactor,
                                               String... commandNames) {
        CompletableFuture<Void> subscriptionResult = CompletableFuture.completedFuture(null);
        for (String commandName : commandNames) {
            commandHandlers.put(commandName, handler);
            logger.info("Registered handler for command {}", commandName);
            String instructionId = UUID.randomUUID().toString();
            doIfNotNull(outboundCommandStream.get(),
                        s -> s.onNext(buildSubscribeMessage(commandName, instructionId, loadFactor)));
        }
        return new AsyncRegistration(subscriptionResult, () -> unsubscribe(handler, commandNames));
    }

    private CompletableFuture<Void> unsubscribe(Function<Command, CompletableFuture<CommandResponse>> handler, String... commandNames) {
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        for (String commandName : commandNames) {
            if (commandHandlers.get(commandName) == handler) {
                CompletableFuture<Void> result = sendUnsubscribe(commandName)
                        .thenRun(() -> commandHandlers.remove(commandName, handler));
                future = CompletableFuture.allOf(future, result);
            }
        }
        return future;
    }

    private CompletableFuture<Void> sendUnsubscribe(String commandName) {
        String instructionId = UUID.randomUUID().toString();
        CompletableFuture<Void> future = new CompletableFuture<>();
        doIfNotNull(outboundCommandStream.get(),
                    s -> {
                        instructionsAwaitingAck.put(instructionId, future);
                        s.onNext(
                                CommandProviderOutbound.newBuilder()
                                                       .setInstructionId(instructionId)
                                                       .setUnsubscribe(CommandSubscription.newBuilder()
                                                                                          .setMessageId(instructionId)
                                                                                          .setCommand(commandName)
                                                                                          .setClientId(clientIdentification.getClientId())
                                                                                          .setComponentName(clientIdentification.getComponentName()))
                                                       .build()
                        );

                    })
                .orElse(() -> future.complete(null));
        return future;
    }

    private CommandProviderOutbound buildSubscribeMessage(String commandName, String instructionId, int loadFactor) {
        return CommandProviderOutbound.newBuilder()
                                      .setInstructionId(instructionId)
                                      .setSubscribe(CommandSubscription.newBuilder()
                                                                       .setMessageId(instructionId)
                                                                       .setCommand(commandName)
                                                                       .setClientId(clientIdentification.getClientId())
                                                                       .setComponentName(clientIdentification
                                                                                                 .getComponentName())
                                                                       .setLoadFactor(loadFactor))
                                      .build();
    }

    @Override
    public CompletableFuture<CommandResponse> sendCommand(Command command) {
        boolean hasRoutingKey = command.getProcessingInstructionsList()
                                       .stream()
                                       .anyMatch(pi -> pi.getKey() == ProcessingKey.ROUTING_KEY);
        String messageIdentifier = "".equals(command.getMessageIdentifier())
                ? UUID.randomUUID().toString()
                : command.getMessageIdentifier();

        Command.Builder toSend = Command.newBuilder(command)
                                        .setMessageIdentifier(messageIdentifier)
                                        .setClientId(clientIdentification.getClientId())
                                        .setComponentName(clientIdentification.getComponentName());
        if (!hasRoutingKey) {
            toSend.addProcessingInstructions(
                    ProcessingInstruction.newBuilder()
                                         .setKey(ProcessingKey.ROUTING_KEY)
                                         .setValue(
                                                 MetaDataValue.newBuilder()
                                                              .setTextValue(toSend.getMessageIdentifier())
                                         )
            );
        }
        CompletableFuture<CommandResponse> response = new CompletableFuture<>();

        try {
            commandServiceStub.dispatch(
                    toSend.build(), new CommandResponseHandler(clientIdentification.getClientId(), response)
            );
        } catch (OutOfDirectMemoryError e) {
            // error thrown when Netty is out of buffer space to send this command
            // unfortunately, the API doesn't allow us to detect this prior to sending
            // TODO - Use a backpressure mechanism when this error occurs, instead of failing directly
            response.completeExceptionally(new AxonServerException(ErrorCategory.COMMAND_DISPATCH_ERROR,
                                                                   "Unable to buffer message for dispatching",
                                                                   clientIdentification.getClientId(),
                                                                   e));
        } catch (Exception e) {
            response.completeExceptionally(new AxonServerException(
                    ErrorCategory.COMMAND_DISPATCH_ERROR,
                    "An error occurred while attempting to dispatch a message",
                    clientIdentification.getClientId(),
                    e
            ));
        }
        return response;
    }

    @Override
    public CompletableFuture<Void> prepareDisconnect() {
        return this.commandHandlers.keySet()
                                   .stream()
                                   .map(this::sendUnsubscribe)
                                   .reduce(CompletableFuture::allOf)
                                   .orElseGet(() -> CompletableFuture.completedFuture(null));
    }

    private static class CommandResponseHandler implements StreamObserver<CommandResponse> {

        private final String clientId;
        private final CompletableFuture<CommandResponse> response;

        public CommandResponseHandler(String clientId, CompletableFuture<CommandResponse> response) {
            this.clientId = clientId;
            this.response = response;
        }

        @Override
        public void onNext(CommandResponse value) {
            if (!response.isDone()) {
                response.complete(value);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (!response.isDone()) {
                // TODO - Check for flow control related errors and do backoff-retry.
                response.completeExceptionally(new AxonServerException(ErrorCategory.COMMAND_DISPATCH_ERROR,
                                                                       "Received exception while dispatching command",
                                                                       clientId,
                                                                       t));
            }
        }

        @Override
        public void onCompleted() {
            if (!response.isDone()) {
                response.completeExceptionally(new AxonServerException(ErrorCategory.COMMAND_DISPATCH_ERROR,
                                                                       "Reply completed without result",
                                                                       clientId));
            }
        }
    }

    private class IncomingCommandStream
            extends AbstractIncomingInstructionStream<CommandProviderInbound, CommandProviderOutbound> {

        public IncomingCommandStream(String clientId,
                                     int permits,
                                     int permitsBatch,
                                     Consumer<Throwable> disconnectHandler) {
            super(clientId, permits, permitsBatch, disconnectHandler);
        }

        @Override
        protected CommandProviderOutbound buildFlowControlMessage(FlowControl flowControl) {
            return CommandProviderOutbound.newBuilder().setFlowControl(flowControl).build();
        }

        @Override
        protected CommandProviderOutbound buildAckMessage(InstructionAck ack) {
            return CommandProviderOutbound.newBuilder().setAck(ack).build();
        }

        @Override
        protected String getInstructionId(CommandProviderInbound value) {
            return value.getInstructionId();
        }

        @Override
        protected InstructionHandler<CommandProviderInbound, CommandProviderOutbound> getHandler(
                CommandProviderInbound request
        ) {
            return handlers.get(request.getRequestCase());
        }

        @Override
        protected boolean unregisterOutboundStream(StreamObserver<CommandProviderOutbound> expected) {
            return outboundCommandStream.compareAndSet(expected, null);
        }
    }
}
