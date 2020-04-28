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
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.connector.ErrorCode;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AbstractIncomingInstructionStream;
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
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.netty.util.internal.OutOfDirectMemoryError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;

public class CommandChannelImpl extends AbstractAxonServerChannel implements CommandChannel {

    private static final Logger logger = LoggerFactory.getLogger(CommandChannel.class);
    private final AtomicReference<CommandServiceGrpc.CommandServiceStub> commandService = new AtomicReference<>();
    private final AtomicReference<StreamObserver<CommandProviderOutbound>> outboundCommandStream = new AtomicReference<>();
    private final ClientIdentification clientIdentification;
    private final ConcurrentMap<String, Function<Command, CompletableFuture<CommandResponse>>> commandHandlers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CompletableFuture<Void>> instructionsAwaitingAck = new ConcurrentHashMap<>();
    private final ConcurrentMap<CommandProviderInbound.RequestCase, BiConsumer<CommandProviderInbound, ReplyChannel<CommandProviderOutbound>>> handlers = new ConcurrentHashMap<>();
    private final int permits;
    private final int permitsBatch;

    public CommandChannelImpl(ClientIdentification clientIdentification,
                              int permits, int permitsBatch, ScheduledExecutorService executor) {
        super(executor);
        this.clientIdentification = clientIdentification;
        this.permits = permits;
        this.permitsBatch = permitsBatch;
        handlers.put(CommandProviderInbound.RequestCase.COMMAND, this::handleIncomingCommand);
        handlers.put(CommandProviderInbound.RequestCase.ACK, this::handleAck);
    }

    private void handleIncomingCommand(CommandProviderInbound message, ReplyChannel<CommandProviderOutbound> outbound) {
        Command command = message.getCommand();
        commandHandlers.get(command.getName())
                       .apply(command)
                       .exceptionally(e -> CommandResponse.newBuilder()
                                                          .setErrorCode(ErrorCode.COMMAND_EXECUTION_ERROR.errorCode())
                                                          .setErrorMessage(ErrorMessage.newBuilder().setMessage(e.getMessage()).build())
                                                          .build())
                       .thenApply(CommandResponse::newBuilder)
                       .thenApply(r -> r.setRequestIdentifier(command.getMessageIdentifier()))
                       .whenComplete((r, e) ->
                                             outbound.send(CommandProviderOutbound.newBuilder().setCommandResponse(r).build()))
                       .thenRun(outbound::markConsumed);
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
    public synchronized void connect(ManagedChannel channel) {
        if (outboundCommandStream.get() != null
                && Optional.of(commandService.get()).map(s -> channel.equals(s.getChannel())).orElse(false)) {
            // we're already connected on this channel
            return;
        }
        CommandServiceGrpc.CommandServiceStub commandServiceStub = CommandServiceGrpc.newStub(channel);
        IncomingCommandStream responseObserver = new IncomingCommandStream(clientIdentification.getClientId(),
                                                                           permits, permitsBatch,
                                                                           () -> scheduleReconnect(channel));
        StreamObserver<CommandProviderOutbound> newValue = commandServiceStub.openStream(responseObserver);

        StreamObserver<CommandProviderOutbound> previous = outboundCommandStream.getAndSet(newValue);

        commandHandlers.keySet().forEach(k -> newValue.onNext(buildSubscribeMessage(k, "")));
        responseObserver.enableFlowControl();


        logger.info("CommandChannel connected, {} command handlers registered", commandHandlers.size());
        ObjectUtils.silently(previous, StreamObserver::onCompleted);

        commandService.getAndSet(commandServiceStub);
    }

    @Override
    public void disconnect() {
        doIfNotNull(outboundCommandStream.getAndSet(null), StreamObserver::onCompleted);
    }

    @Override
    public boolean isConnected() {
        return outboundCommandStream.get() != null;
    }

    @Override
    public Runnable registerCommandHandler(Function<Command, CompletableFuture<CommandResponse>> handler, String... commandNames) {
        for (String commandName : commandNames) {
            commandHandlers.put(commandName, handler);
            logger.info("Registered handler for command {}", commandName);
            String instructionId = UUID.randomUUID().toString();
            doIfNotNull(outboundCommandStream.get(),
                        s -> s.onNext(buildSubscribeMessage(commandName, instructionId)));
        }
        return () -> {
            for (String commandName : commandNames) {
                if (commandHandlers.remove(commandName) == handler) {
                    outboundCommandStream.get().onNext(
                            CommandProviderOutbound.newBuilder()
                                                   .setUnsubscribe(CommandSubscription.newBuilder()
                                                                                      .setCommand(commandName)
                                                                                      .setClientId(clientIdentification.getClientId())
                                                                                      .setComponentName(clientIdentification.getComponentName()))
                                                   .build()
                    );
                }
            }
        };
    }

    private CommandProviderOutbound buildSubscribeMessage(String commandName, String instructionId) {
        return CommandProviderOutbound.newBuilder()
                                      .setInstructionId(instructionId)
                                      .setSubscribe(CommandSubscription.newBuilder()
                                                                       .setMessageId(instructionId)
                                                                       .setCommand(commandName)
                                                                       .setClientId(clientIdentification.getClientId())
                                                                       .setComponentName(clientIdentification.getComponentName())
                                                                       .setLoadFactor(100))
                                      .build();
    }

    @Override
    public CompletableFuture<CommandResponse> sendCommand(Command command) {
        boolean hasRoutingKey = command.getProcessingInstructionsList().stream().anyMatch(pi -> pi.getKey() == ProcessingKey.ROUTING_KEY);
        Command.Builder toSend = Command.newBuilder(command)
                                        .setMessageIdentifier("".equals(command.getMessageIdentifier()) ? UUID.randomUUID().toString() : command.getMessageIdentifier())
                                        .setClientId(clientIdentification.getClientId())
                                        .setComponentName(clientIdentification.getComponentName());
        if (!hasRoutingKey) {
            toSend.addProcessingInstructions(ProcessingInstruction.newBuilder().setKey(ProcessingKey.ROUTING_KEY)
                                                                  .setValue(MetaDataValue.newBuilder().setTextValue(toSend.getMessageIdentifier())));
        }
        CompletableFuture<CommandResponse> response = new CompletableFuture<>();


        try {
            CommandServiceGrpc.CommandServiceStub commandServiceStub = commandService.get();
            if (commandServiceStub == null) {
                throw new IllegalStateException("CommandChannel is not connected.");
            }
            commandServiceStub.dispatch(toSend.build(), new CommandResponseHandler(response));
        } catch (OutOfDirectMemoryError e) {
            // error thrown when Netty is out of buffer space to send this command
            // unfortunately, the API doesn't allow us to detect this prior to sending
            // TODO - Use a backpressure mechanism when this error occurs, instead of failing directly
            response.completeExceptionally(new AxonServerException(ErrorCode.COMMAND_DISPATCH_ERROR, "Unable to buffer message for dispatching", e));
        } catch (Exception e) {
            response.completeExceptionally(new AxonServerException(ErrorCode.COMMAND_DISPATCH_ERROR, "An error occurred while attempting to dispatch a message", e));
        }
        return response;
    }

    private static class CommandResponseHandler implements StreamObserver<CommandResponse> {

        private final CompletableFuture<CommandResponse> response;

        public CommandResponseHandler(CompletableFuture<CommandResponse> response) {
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
                response.completeExceptionally(new AxonServerException(ErrorCode.COMMAND_DISPATCH_ERROR,
                                                                       "Received exception while dispatching command",
                                                                       t));
            }
        }

        @Override
        public void onCompleted() {
            if (!response.isDone()) {
                response.completeExceptionally(new AxonServerException(ErrorCode.COMMAND_DISPATCH_ERROR,
                                                                       "Reply completed without result"));
            }
        }
    }

    private class IncomingCommandStream extends AbstractIncomingInstructionStream<CommandProviderInbound, CommandProviderOutbound> {

        public IncomingCommandStream(String clientId, int permits, int permitsBatch, Runnable disconnectHandler) {
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
        protected BiConsumer<CommandProviderInbound, ReplyChannel<CommandProviderOutbound>> getHandler(CommandProviderInbound request) {
            return handlers.get(request.getRequestCase());
        }

        @Override
        protected boolean replaceOutBoundStream(StreamObserver<CommandProviderOutbound> expected, StreamObserver<CommandProviderOutbound> replaceBy) {
            return outboundCommandStream.compareAndSet(expected, replaceBy);
        }
    }
}
