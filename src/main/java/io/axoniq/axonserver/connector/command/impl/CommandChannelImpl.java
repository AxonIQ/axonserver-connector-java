/*
 * Copyright (c) 2020-2022. AxonIQ
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
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.util.internal.OutOfDirectMemoryError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;

/**
 * {@link CommandChannel} implementation, serving as the command connection between AxonServer and a client
 * application.
 */
public class CommandChannelImpl extends AbstractAxonServerChannel<CommandProviderOutbound> implements CommandChannel {

    private static final Logger logger = LoggerFactory.getLogger(CommandChannelImpl.class);

    private final AtomicReference<CallStreamObserver<CommandProviderOutbound>> outboundCommandStream = new AtomicReference<>();
    private final ClientIdentification clientIdentification;
    private final ConcurrentMap<String, CommandHandler> commandHandlers = new ConcurrentHashMap<>();
    private final ConcurrentMap<CommandProviderInbound.RequestCase, InstructionHandler<CommandProviderInbound, CommandProviderOutbound>> handlers = new ConcurrentHashMap<>();
    private final int permits;
    private final int permitsBatch;
    private final CommandServiceGrpc.CommandServiceStub commandServiceStub;

    private final CommandHandler noCommandHandler = new CommandHandler(c -> noHandlerForCommand(), 0);
    private final String context;
    private final Set<CompletableFuture<?>> inProgressCommands = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean subscriptionsCompleted = new AtomicBoolean(false);

    /**
     * Constructs a {@link CommandChannelImpl}.
     *
     * @param clientIdentification client information identifying whom has connected. This information is used to pass
     *                             on to message
     * @param permits              an {@code int} defining the number of permits this channel has
     * @param permitsBatch         an {@code int} defining the number of permits to be consumed from prior to requesting
     *                             additional permits for this channel
     * @param executor             a {@link ScheduledExecutorService} used to schedule reconnects of this channel
     * @param channel              the {@link AxonServerManagedChannel} used to form the connection with AxonServer
     */
    public CommandChannelImpl(ClientIdentification clientIdentification,
                              String context,
                              int permits,
                              int permitsBatch,
                              ScheduledExecutorService executor,
                              AxonServerManagedChannel channel) {
        super(clientIdentification, executor, channel);
        this.clientIdentification = clientIdentification;
        this.context = context;
        this.permits = permits;
        this.permitsBatch = permitsBatch;
        this.handlers.put(CommandProviderInbound.RequestCase.COMMAND, this::handleIncomingCommand);
        this.handlers.put(CommandProviderInbound.RequestCase.ACK, this::handleAck);
        this.commandServiceStub = CommandServiceGrpc.newStub(channel);
    }

    private void handleIncomingCommand(CommandProviderInbound message, ReplyChannel<CommandProviderOutbound> outbound) {
        Command command = message.getCommand();
        CommandHandler handler = commandHandlers.get(command.getName());
        if (handler == null) {
            handler = noCommandHandler;
        }

        CompletableFuture<Void> result = handler.getHandler().apply(command)
                                                .thenApply(CommandResponse::newBuilder)
                                                .exceptionally(e -> CommandResponse.newBuilder()
                                                                                   .setErrorCode(ErrorCategory.COMMAND_EXECUTION_ERROR.errorCode())
                                                                                   .setErrorMessage(
                                                                                           ErrorMessage.newBuilder()
                                                                                                       .setMessage(e.getMessage()))
                                                )
                                                .thenApply(r -> r.setRequestIdentifier(command.getMessageIdentifier()))
                                                .whenComplete((r, e) -> outbound.send(
                                                        CommandProviderOutbound.newBuilder().setCommandResponse(r).build()
                                                ))
                                                .thenRun(outbound::complete);
        inProgressCommands.add(result);
        result.whenComplete((r, e) -> inProgressCommands.remove(result));
    }

    private CompletableFuture<CommandResponse> noHandlerForCommand() {
        CompletableFuture<CommandResponse> r = new CompletableFuture<>();
        r.completeExceptionally(new AxonServerException(ErrorCategory.NO_HANDLER_FOR_COMMAND,
                                                        "No handler for command",
                                                        clientIdentification.getClientId()));
        return r;
    }

    private void handleAck(CommandProviderInbound message, ReplyChannel<CommandProviderOutbound> outbound) {
        processAck(message.getAck());
        outbound.complete();
    }

    @Override
    public void connect() {
        if (!commandHandlers.isEmpty()) {
            logger.debug("CommandChannel for context '{}' will attempt to connect.", context);
            doCreateCommandStream();
        }
    }

    private synchronized void doCreateCommandStream() {
        if (outboundCommandStream.get() != null) {
            logger.debug("CommandChannel for context '{}' is already connected.", context);
            return;
        }

        this.subscriptionsCompleted.set(commandHandlers.isEmpty());

        IncomingCommandStream responseObserver = new IncomingCommandStream(
                clientIdentification.getClientId(), permits, permitsBatch,
                this::scheduleReconnect,
                this::registerOutboundStream
        );

        try {
            //noinspection ResultOfMethodCallIgnored
            commandServiceStub.openStream(responseObserver);
        } catch (Exception e) {
            logger.warn("Failed trying to open stream for CommandChannel in context [{}]. "
                                + "Cleaning up for following attempt...", context, e);
            outboundCommandStream.set(null);
            throw e;
        }

        StreamObserver<CommandProviderOutbound> newValue = responseObserver.getInstructionsForPlatform();

        commandHandlers.entrySet().stream()
                       .map(e -> sendSubscribe(e.getKey(), e.getValue().getLoadFactor(), newValue))
                       .reduce(CompletableFuture::allOf)
                       .orElse(CompletableFuture.completedFuture(null))
                       .whenComplete((unused, throwable) -> {
                           if (throwable != null) {
                               logger.warn("An error occurred while registering command handlers", throwable);
                           } else {
                               logger.info("CommandChannel for context '{}' connected, {} command handlers registered", context, commandHandlers.size());
                           }
                           subscriptionsCompleted.set(throwable == null);
                       });


        responseObserver.enableFlowControl();
    }

    private void registerOutboundStream(CallStreamObserver<CommandProviderOutbound> upstream) {
        StreamObserver<CommandProviderOutbound> previous = outboundCommandStream.getAndSet(upstream);
        ObjectUtils.silently(previous, StreamObserver::onCompleted);
    }

    @Override
    public void reconnect() {
        logger.debug("Reconnecting CommandChannel for context '{}'.", context);
        disconnect();
        scheduleImmediateReconnect();
    }

    @Override
    public void disconnect() {
        logger.debug("Disconnecting CommandChannel for context '{}'.", context);
        StreamObserver<CommandProviderOutbound> previousOutbound = outboundCommandStream.getAndSet(null);

        CompletableFuture<Void> unsubscribed = previousOutbound == null
                                               ? CompletableFuture.completedFuture(null)
                                               : commandHandlers.keySet()
                                                                .stream()
                                                                .map(commandName -> sendUnsubscribe(commandName, previousOutbound))
                                                                .reduce(CompletableFuture::allOf)
                                                                .map(cf -> cf.exceptionally(e -> {
                                                                    logger.warn("An error occurred while unregistering command handlers", e);
                                                                    return null;
                                                                }))
                                                                .orElseGet(() -> CompletableFuture.completedFuture(null));

        unsubscribed
                .thenCompose(r -> {
                    if (!inProgressCommands.isEmpty()) {
                        logger.info("Waiting for {} commands to be completed", inProgressCommands.size());
                    }
                    return CompletableFuture.allOf(inProgressCommands.stream()
                                                                     .reduce(CompletableFuture::allOf)
                                                                     .map(cf -> cf.exceptionally(e -> null))
                                                                     .orElseGet(() -> CompletableFuture.completedFuture(null)));
                })
                .thenRun(() -> doIfNotNull(previousOutbound, StreamObserver::onCompleted));
        subscriptionsCompleted.set(false);
    }

    @Override
    public boolean isReady() {
        return commandHandlers.isEmpty() || (outboundCommandStream.get() != null && subscriptionsCompleted.get());
    }

    @Override
    public Registration registerCommandHandler(Function<Command, CompletableFuture<CommandResponse>> handler,
                                               int loadFactor,
                                               String... commandNames) {
        if (commandHandlers.isEmpty()) {
            doCreateCommandStream();
        }
        CompletableFuture<Void> subscriptionResult = CompletableFuture.completedFuture(null);
        CommandHandler commandHandler = new CommandHandler(handler, loadFactor);
        for (String commandName : commandNames) {
            commandHandlers.put(commandName, commandHandler);
            logger.info("Registered handler for command '{}' in context '{}'", commandName, context);
            CompletableFuture<Void> ack = sendSubscribe(commandName, loadFactor, outboundCommandStream.get());
            subscriptionResult = CompletableFuture.allOf(subscriptionResult, ack);
        }
        return new AsyncRegistration(subscriptionResult, () -> unsubscribe(commandHandler, commandNames));
    }

    private CompletableFuture<Void> sendSubscribe(String commandName, int loadFactor, StreamObserver<CommandProviderOutbound> outbound) {
        String instructionId = UUID.randomUUID().toString();
        return sendInstruction(CommandProviderOutbound.newBuilder()
                                                      .setInstructionId(instructionId)
                                                      .setSubscribe(CommandSubscription.newBuilder()
                                                                                       .setMessageId(instructionId)
                                                                                       .setCommand(commandName)
                                                                                       .setClientId(clientIdentification.getClientId())
                                                                                       .setComponentName(clientIdentification
                                                                                                                 .getComponentName())
                                                                                       .setLoadFactor(loadFactor))
                                                      .build(),
                               CommandProviderOutbound::getInstructionId,
                               outbound);
    }

    private CompletableFuture<Void> unsubscribe(CommandHandler handler,
                                                String... commandNames) {
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        for (String commandName : commandNames) {
            if (commandHandlers.get(commandName) == handler) {
                logger.info("Unregistered handler for command '{}' in context '{}'", commandName, context);
                CompletableFuture<Void> result = sendUnsubscribe(commandName, outboundCommandStream.get())
                        .thenRun(() -> commandHandlers.remove(commandName, handler));
                future = CompletableFuture.allOf(future, result);
            }
        }
        return future;
    }

    private CompletableFuture<Void> sendUnsubscribe(String commandName, StreamObserver<CommandProviderOutbound> outbound) {
        String instructionId = UUID.randomUUID().toString();
        CommandSubscription unsubscribeMessage =
                CommandSubscription.newBuilder()
                                   .setMessageId(instructionId)
                                   .setCommand(commandName)
                                   .setClientId(clientIdentification.getClientId())
                                   .setComponentName(clientIdentification.getComponentName())
                                   .build();
        return sendInstruction(CommandProviderOutbound.newBuilder()
                                                      .setInstructionId(instructionId)
                                                      .setUnsubscribe(unsubscribeMessage)
                                                      .build(),
                               CommandProviderOutbound::getInstructionId,
                               outbound);
    }

    @Override
    public CompletableFuture<CommandResponse> sendCommand(Command command) {
        logger.trace("Sending command over CommandChannel for context '{}'.", context);
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
            // TODO - Use a backpressure mechanism when this error occurs, instead of failing directly - issue #3
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
        logger.debug("Preparing disconnect on CommandChannel for context '{}'.", context);
        return this.commandHandlers.keySet()
                                   .stream()
                                   .map(commandName -> sendUnsubscribe(commandName, outboundCommandStream.get()))
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
                // TODO - Check for flow control related errors and do backoff-retry - issue #3
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

    private static class CommandHandler {

        private final Function<Command, CompletableFuture<CommandResponse>> handler;
        private final int loadFactor;

        public CommandHandler(Function<Command, CompletableFuture<CommandResponse>> handler, int loadFactor) {
            this.handler = handler;
            this.loadFactor = loadFactor;
        }

        public Function<Command, CompletableFuture<CommandResponse>> getHandler() {
            return handler;
        }

        public int getLoadFactor() {
            return loadFactor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CommandHandler that = (CommandHandler) o;
            return handler.equals(that.handler);
        }

        @Override
        public int hashCode() {
            return Objects.hash(handler);
        }
    }

    private class IncomingCommandStream
            extends AbstractIncomingInstructionStream<CommandProviderInbound, CommandProviderOutbound> {

        public IncomingCommandStream(String clientId,
                                     int permits,
                                     int permitsBatch,
                                     Consumer<Throwable> disconnectHandler,
                                     Consumer<CallStreamObserver<CommandProviderOutbound>> beforeStartHandler) {
            super(clientId, permits, permitsBatch, disconnectHandler, beforeStartHandler);
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
        protected String getInstructionId(CommandProviderInbound instruction) {
            return instruction.getInstructionId();
        }

        @Override
        protected InstructionHandler<CommandProviderInbound, CommandProviderOutbound> getHandler(
                CommandProviderInbound request
        ) {
            return handlers.get(request.getRequestCase());
        }

        @Override
        protected boolean unregisterOutboundStream(CallStreamObserver<CommandProviderOutbound> expected) {
            return outboundCommandStream.compareAndSet(expected, null);
        }
    }
}
