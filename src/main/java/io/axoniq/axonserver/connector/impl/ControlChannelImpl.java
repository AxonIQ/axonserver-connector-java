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

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.InstructionHandler;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.control.ControlChannel;
import io.axoniq.axonserver.connector.control.ProcessorInstructionHandler;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.Heartbeat;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformServiceGrpc;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;
import static io.axoniq.axonserver.connector.impl.ObjectUtils.hasLength;
import static io.axoniq.axonserver.connector.impl.ObjectUtils.silently;

/**
 * {@link ControlChannel} implementation, serving as the overall control and instruction connection between AxonServer
 * and a client application.
 */
public class ControlChannelImpl extends AbstractAxonServerChannel implements ControlChannel {

    private static final Logger logger = LoggerFactory.getLogger(ControlChannelImpl.class);

    private final ClientIdentification clientIdentification;
    private final ScheduledExecutorService executor;
    private final long processorInfoUpdateFrequency;
    private final Runnable reconnectHandler;
    private final AtomicReference<CallStreamObserver<PlatformInboundInstruction>> instructionDispatcher = new AtomicReference<>();
    private final Map<PlatformOutboundInstruction.RequestCase, InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction>> instructionHandlers =
            new EnumMap<>(PlatformOutboundInstruction.RequestCase.class);
    private final HeartbeatMonitor heartbeatMonitor;
    private final Map<String, CompletableFuture<InstructionAck>> awaitingAck = new ConcurrentHashMap<>();
    private final String context;
    private final Map<String, ProcessorInstructionHandler> processorInstructionHandlers = new ConcurrentHashMap<>();
    private final Map<String, Supplier<EventProcessorInfo>> processorInfoSuppliers = new ConcurrentHashMap<>();
    private final AtomicBoolean infoSupplierActive = new AtomicBoolean();
    private final PlatformServiceGrpc.PlatformServiceStub platformServiceStub;

    /**
     * Constructs a {@link ControlChannelImpl}.
     *
     * @param clientIdentification         the information identifying the client application which is connecting. This
     *                                     information is used to pass on to message
     * @param context                      the context this {@link ControlChannel} operates in
     * @param executor                     a {@link ScheduledExecutorService} used to schedule reconnects of this
     *                                     channel, heartbeat message dispatching and processor information updates
     * @param channel                      the {@link AxonServerManagedChannel} used to form the connection with
     *                                     AxonServer
     * @param processorInfoUpdateFrequency the update frequency in milliseconds of event processor information
     * @param reconnectHandler             operation run when a reconnect request is received for this channel
     */
    public ControlChannelImpl(ClientIdentification clientIdentification,
                              String context,
                              ScheduledExecutorService executor,
                              AxonServerManagedChannel channel,
                              long processorInfoUpdateFrequency,
                              Runnable reconnectHandler) {
        super(executor, channel);
        this.clientIdentification = clientIdentification;
        this.context = context;
        this.executor = executor;
        this.processorInfoUpdateFrequency = processorInfoUpdateFrequency;
        this.reconnectHandler = reconnectHandler;
        heartbeatMonitor = new HeartbeatMonitor(executor, this::sendHeartBeat, channel::forceReconnect);
        this.instructionHandlers.computeIfAbsent(
                PlatformOutboundInstruction.RequestCase.ACK,
                i -> new AckHandler()
        );
        this.instructionHandlers.computeIfAbsent(
                PlatformOutboundInstruction.RequestCase.HEARTBEAT,
                i -> (msg, reply) -> heartbeatMonitor.handleIncomingBeat(reply)
        );
        this.instructionHandlers.computeIfAbsent(
                PlatformOutboundInstruction.RequestCase.MERGE_EVENT_PROCESSOR_SEGMENT,
                i -> ProcessorInstructions.mergeHandler(processorInstructionHandlers)
        );
        this.instructionHandlers.computeIfAbsent(
                PlatformOutboundInstruction.RequestCase.SPLIT_EVENT_PROCESSOR_SEGMENT,
                i -> ProcessorInstructions.splitHandler(processorInstructionHandlers)
        );
        this.instructionHandlers.computeIfAbsent(
                PlatformOutboundInstruction.RequestCase.START_EVENT_PROCESSOR,
                i -> ProcessorInstructions.startHandler(processorInstructionHandlers)
        );
        this.instructionHandlers.computeIfAbsent(
                PlatformOutboundInstruction.RequestCase.PAUSE_EVENT_PROCESSOR,
                i -> ProcessorInstructions.pauseHandler(processorInstructionHandlers)
        );
        this.instructionHandlers.computeIfAbsent(
                PlatformOutboundInstruction.RequestCase.RELEASE_SEGMENT,
                i -> ProcessorInstructions.releaseSegmentHandler(processorInstructionHandlers)
        );
        this.instructionHandlers.computeIfAbsent(
                PlatformOutboundInstruction.RequestCase.REQUEST_EVENT_PROCESSOR_INFO,
                i -> ProcessorInstructions.requestInfoHandler(processorInfoSuppliers)
        );
        this.instructionHandlers.computeIfAbsent(
                PlatformOutboundInstruction.RequestCase.REQUEST_RECONNECT,
                i -> this::handleReconnectRequest
        );
        platformServiceStub = PlatformServiceGrpc.newStub(channel);
    }

    private CompletableFuture<InstructionAck> sendHeartBeat() {
        if (!isConnected()) {
            return CompletableFuture.completedFuture(null);
        }
        PlatformInboundInstruction heartbeatMessage =
                PlatformInboundInstruction.newBuilder()
                                          .setInstructionId(UUID.randomUUID().toString())
                                          .setHeartbeat(Heartbeat.getDefaultInstance())
                                          .build();
        return sendInstruction(heartbeatMessage);
    }

    /* visible for testing */
    void handleReconnectRequest(PlatformOutboundInstruction platformOutboundInstruction,
                                ReplyChannel<PlatformInboundInstruction> replyChannel) {
        logger.info("AxonServer requested reconnect for context '{}'", context);
        replyChannel.sendAck();
        reconnectHandler.run();
    }

    @Override
    public synchronized void connect() {
        StreamObserver<PlatformInboundInstruction> existing = instructionDispatcher.get();
        if (existing != null) {
            logger.info("ControlChannel for context '{}' is already connected", context);
        } else {
            PlatformOutboundInstructionHandler responseObserver =
                    new PlatformOutboundInstructionHandler(clientIdentification.getClientId(), this::handleDisconnect,
                                                           upstream -> {
                                                               StreamObserver<PlatformInboundInstruction> previous =
                                                                       instructionDispatcher.getAndSet(upstream);
                                                               silently(previous, StreamObserver::onCompleted);
                                                           });
            logger.debug("Opening instruction stream for context '{}'", context);
            //noinspection ResultOfMethodCallIgnored
            platformServiceStub.openStream(responseObserver);
            StreamObserver<PlatformInboundInstruction> instructionsForPlatform =
                    responseObserver.getInstructionsForPlatform();

            try {
                logger.info("Connected instruction stream for context '{}'. Sending client identification", context);
                instructionsForPlatform.onNext(PlatformInboundInstruction.newBuilder()
                                                                         .setRegister(clientIdentification)
                                                                         .build());
                heartbeatMonitor.resume();
            } catch (Exception e) {
                instructionDispatcher.set(null);
                instructionsForPlatform.onError(e);
            }
        }
    }

    @Override
    public void reconnect() {
        doIfNotNull(instructionDispatcher.getAndSet(null), StreamObserver::onCompleted);
        scheduleImmediateReconnect();
    }

    private void handleDisconnect(Throwable cause) {
        failOpenInstructions(cause);
        scheduleReconnect();
    }

    private void failOpenInstructions(Throwable cause) {
        while (!awaitingAck.isEmpty()) {
            awaitingAck.keySet()
                       .forEach(k -> doIfNotNull(awaitingAck.remove(k),
                                                 cf -> cf.completeExceptionally(cause)));
        }
    }

    @Override
    public void disconnect() {
        heartbeatMonitor.disableHeartbeat();
        StreamObserver<PlatformInboundInstruction> dispatcher = instructionDispatcher.getAndSet(null);
        if (dispatcher != null) {
            dispatcher.onCompleted();
        }
    }

    @Override
    public Registration registerInstructionHandler(PlatformOutboundInstruction.RequestCase type,
                                                   InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> handler) {
        instructionHandlers.put(type, handler);
        return new SyncRegistration(() -> instructionHandlers.remove(type, handler));
    }

    @Override
    public Registration registerEventProcessor(String processorName,
                                               Supplier<EventProcessorInfo> infoSupplier,
                                               ProcessorInstructionHandler instructionHandler) {
        processorInstructionHandlers.put(processorName, instructionHandler);
        processorInfoSuppliers.put(processorName, infoSupplier);
        if (infoSupplierActive.compareAndSet(false, true)) {
            // first processor is registered
            sendScheduledProcessorInfo();
        }
        return new SyncRegistration(() -> {
            processorInstructionHandlers.remove(processorName, instructionHandler);
            processorInfoSuppliers.remove(processorName, infoSupplier);
        });
    }

    private void sendScheduledProcessorInfo() {
        Collection<Supplier<EventProcessorInfo>> infoSupplies = processorInfoSuppliers.values();
        if (infoSupplies.isEmpty()) {
            infoSupplierActive.set(false);
            // a processor may have been registered simultaneously
            if (!processorInfoSuppliers.isEmpty() && infoSupplierActive.compareAndSet(false, true)) {
                sendScheduledProcessorInfo();
            }
        } else {
            CallStreamObserver<PlatformInboundInstruction> outbound = instructionDispatcher.get();
            if (outbound != null && outbound.isReady()) {
                infoSupplies.forEach(info -> doIfNotNull(info.get(), this::sendProcessorInfo));
            }
            executor.schedule(this::sendScheduledProcessorInfo, processorInfoUpdateFrequency, TimeUnit.MILLISECONDS);
        }
    }

    private CompletableFuture<InstructionAck> sendProcessorInfo(EventProcessorInfo processorInfo) {
        return sendInstruction(PlatformInboundInstruction.newBuilder().setEventProcessorInfo(processorInfo).build());
    }

    @Override
    public void enableHeartbeat(long interval, long timeout, TimeUnit timeUnit) {
        heartbeatMonitor.enableHeartbeat(interval, timeout, timeUnit);
    }

    @Override
    public void disableHeartbeat() {
        heartbeatMonitor.disableHeartbeat();
    }

    @Override
    public CompletableFuture<InstructionAck> sendInstruction(PlatformInboundInstruction instruction) {
        CompletableFuture<InstructionAck> result = new CompletableFuture<>();
        String instructionId = instruction.getInstructionId();
        StreamObserver<PlatformInboundInstruction> dispatcher = instructionDispatcher.get();
        if (dispatcher == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Unable to send instruction: {} {}. Disconnected.",
                             instruction.getRequestCase().name(),
                             instructionId);
            }
            result.completeExceptionally(new AxonServerException(ErrorCategory.INSTRUCTION_EXECUTION_ERROR,
                                                                 "Unable to send instruction",
                                                                 clientIdentification.getClientId()));
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Sending instruction: {} {}", instruction.getRequestCase().name(), instructionId);
            }
            if (hasLength(instructionId)) {
                awaitingAck.put(instructionId, result);
            }
            try {
                dispatcher.onNext(instruction);
                if (!hasLength(instructionId)) {
                    result.complete(InstructionAck.newBuilder().setSuccess(true).build());
                }
            } catch (Exception e) {
                awaitingAck.remove(instructionId);
                result.completeExceptionally(e);
                silently(dispatcher, d -> d.onError(e));
            }
        }
        return result;
    }

    @Override
    public boolean isConnected() {
        return instructionDispatcher.get() != null;
    }

    private class PlatformOutboundInstructionHandler
            extends AbstractIncomingInstructionStream<PlatformOutboundInstruction, PlatformInboundInstruction> {

        public PlatformOutboundInstructionHandler(String clientId,
                                                  Consumer<Throwable> disconnectHandler,
                                                  Consumer<CallStreamObserver<PlatformInboundInstruction>> onStartHandler) {
            super(clientId, 0, 0, disconnectHandler, onStartHandler);
        }

        @Override
        protected PlatformInboundInstruction buildAckMessage(InstructionAck ack) {
            return PlatformInboundInstruction.newBuilder().setAck(ack).build();
        }

        @Override
        protected String getInstructionId(PlatformOutboundInstruction instruction) {
            return instruction.getInstructionId();
        }


        @Override
        protected InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> getHandler(
                PlatformOutboundInstruction platformOutboundInstruction
        ) {
            return instructionHandlers.get(platformOutboundInstruction.getRequestCase());
        }

        @Override
        protected boolean unregisterOutboundStream(CallStreamObserver<PlatformInboundInstruction> expected) {
            heartbeatMonitor.pause();
            boolean disconnected = instructionDispatcher.compareAndSet(expected, null);
            if (disconnected) {
                failOpenInstructions(new AxonServerException(
                        ErrorCategory.INSTRUCTION_ACK_ERROR,
                        "Disconnected from AxonServer before receiving instruction ACK",
                        clientId()
                ));
            }
            return disconnected;
        }

        @Override
        protected PlatformInboundInstruction buildFlowControlMessage(FlowControl flowControl) {
            return null;
        }
    }

    private class AckHandler implements InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> {

        @Override
        public void handle(PlatformOutboundInstruction ackMessage,
                           ReplyChannel<PlatformInboundInstruction> replyChannel) {
            String instructionId = ackMessage.getAck().getInstructionId();
            logger.debug("Received ACK for {}", instructionId);
            CompletableFuture<InstructionAck> handle = awaitingAck.remove(instructionId);
            if (handle != null) {
                handle.complete(ackMessage.getAck());
            }
        }
    }
}
