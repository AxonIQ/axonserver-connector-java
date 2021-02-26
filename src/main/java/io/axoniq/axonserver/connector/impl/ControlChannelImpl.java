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
import static io.axoniq.axonserver.connector.impl.ObjectUtils.silently;

/**
 * {@link ControlChannel} implementation, serving as the overall control and instruction connection between AxonServer
 * and a client application.
 */
public class ControlChannelImpl extends AbstractAxonServerChannel<PlatformInboundInstruction> implements ControlChannel {

    private static final Logger logger = LoggerFactory.getLogger(ControlChannelImpl.class);

    private final ClientIdentification clientIdentification;
    private final ScheduledExecutorService executor;
    private final long processorInfoUpdateFrequency;
    private final Runnable reconnectHandler;
    private final AtomicReference<CallStreamObserver<PlatformInboundInstruction>> instructionDispatcher = new AtomicReference<>();
    private final Map<PlatformOutboundInstruction.RequestCase, InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction>> instructionHandlers =
            new EnumMap<>(PlatformOutboundInstruction.RequestCase.class);
    private final HeartbeatMonitor heartbeatMonitor;
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
        super(clientIdentification, executor, channel);
        this.clientIdentification = clientIdentification;
        this.context = context;
        this.executor = executor;
        this.processorInfoUpdateFrequency = processorInfoUpdateFrequency;
        this.reconnectHandler = reconnectHandler;
        this.heartbeatMonitor = new HeartbeatMonitor(executor, this::sendHeartBeat, channel::requestReconnect);
        this.instructionHandlers.put(PlatformOutboundInstruction.RequestCase.ACK, this::handleAck);
        this.instructionHandlers.put(PlatformOutboundInstruction.RequestCase.HEARTBEAT,
                                     (msg, reply) -> heartbeatMonitor.handleIncomingBeat(reply));
        this.instructionHandlers.put(PlatformOutboundInstruction.RequestCase.MERGE_EVENT_PROCESSOR_SEGMENT,
                                     ProcessorInstructions.mergeHandler(processorInstructionHandlers));
        this.instructionHandlers.put(PlatformOutboundInstruction.RequestCase.SPLIT_EVENT_PROCESSOR_SEGMENT,
                                     ProcessorInstructions.splitHandler(processorInstructionHandlers));
        this.instructionHandlers.put(PlatformOutboundInstruction.RequestCase.START_EVENT_PROCESSOR,
                                     ProcessorInstructions.startHandler(processorInstructionHandlers));
        this.instructionHandlers.put(PlatformOutboundInstruction.RequestCase.PAUSE_EVENT_PROCESSOR,
                                     ProcessorInstructions.pauseHandler(processorInstructionHandlers));
        this.instructionHandlers.put(PlatformOutboundInstruction.RequestCase.RELEASE_SEGMENT,
                                     ProcessorInstructions.releaseSegmentHandler(processorInstructionHandlers));
        this.instructionHandlers.put(PlatformOutboundInstruction.RequestCase.REQUEST_EVENT_PROCESSOR_INFO,
                                     ProcessorInstructions.requestInfoHandler(processorInfoSuppliers));
        this.instructionHandlers.put(PlatformOutboundInstruction.RequestCase.REQUEST_RECONNECT,
                                     this::handleReconnectRequest);

        platformServiceStub = PlatformServiceGrpc.newStub(channel);
    }

    private void handleAck(PlatformOutboundInstruction instruction, ReplyChannel<PlatformInboundInstruction> replyChannel) {
        processAck(instruction.getAck());
        replyChannel.complete();
    }

    private CompletableFuture<Void> sendHeartBeat() {
        if (!isReady()) {
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
                    new PlatformOutboundInstructionHandler(clientIdentification.getClientId(),
                                                           this::handleDisconnect,
                                                           this::registerOutboundStream);
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

    private void registerOutboundStream(CallStreamObserver<PlatformInboundInstruction> upstream) {
        StreamObserver<PlatformInboundInstruction> previous =
                instructionDispatcher.getAndSet(upstream);
        silently(previous, StreamObserver::onCompleted);
    }

    @Override
    public synchronized void reconnect() {
        doIfNotNull(instructionDispatcher.getAndSet(null), StreamObserver::onCompleted);
        scheduleImmediateReconnect();
    }

    private void handleDisconnect(Throwable cause) {
        heartbeatMonitor.pause();
        scheduleReconnect(cause);
    }

    @Override
    public synchronized void disconnect() {
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
            } else {
                logger.debug("Not sending processor info for context '{}'. Channel not ready...", context);
            }
            executor.schedule(this::sendScheduledProcessorInfo, processorInfoUpdateFrequency, TimeUnit.MILLISECONDS);
        }
    }

    private CompletableFuture<Void> sendProcessorInfo(EventProcessorInfo processorInfo) {
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
    public CompletableFuture<Void> sendInstruction(PlatformInboundInstruction instruction) {
        return sendInstruction(instruction,
                               PlatformInboundInstruction::getInstructionId,
                               instructionDispatcher.get());
    }

    @Override
    public boolean isReady() {
        return instructionDispatcher.get() != null;
    }

    @Override
    protected boolean shutdownOnUnavailable() {
        return true;
    }

    private class PlatformOutboundInstructionHandler
            extends AbstractIncomingInstructionStream<PlatformOutboundInstruction, PlatformInboundInstruction> {

        public PlatformOutboundInstructionHandler(String clientId,
                                                  Consumer<Throwable> disconnectHandler,
                                                  Consumer<CallStreamObserver<PlatformInboundInstruction>> beforeStartHandler) {
            super(clientId, 0, 0, disconnectHandler, beforeStartHandler);
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
            return instructionDispatcher.compareAndSet(expected, null);
        }

        @Override
        protected PlatformInboundInstruction buildFlowControlMessage(FlowControl flowControl) {
            return null;
        }
    }

}
