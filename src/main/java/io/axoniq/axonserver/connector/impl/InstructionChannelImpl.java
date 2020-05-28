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

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCode;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.instruction.InstructionChannel;
import io.axoniq.axonserver.connector.instruction.InstructionHandler;
import io.axoniq.axonserver.connector.instruction.ProcessorInstructionHandler;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.Heartbeat;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;
import static io.axoniq.axonserver.connector.impl.ObjectUtils.silently;

public class InstructionChannelImpl extends AbstractAxonServerChannel implements InstructionChannel {

    private static final Logger logger = LoggerFactory.getLogger(InstructionChannel.class);
    private final ClientIdentification clientIdentification;
    private final ScheduledExecutorService executor;
    private final AtomicReference<StreamObserver<PlatformInboundInstruction>> instructionDispatcher = new AtomicReference<>();
    private final Map<PlatformOutboundInstruction.RequestCase, BiConsumer<PlatformOutboundInstruction, ReplyChannel<PlatformInboundInstruction>>> instructionHandlers = new HashMap<>();
    private final HeartbeatMonitor heartbeatMonitor;
    private final Map<String, CompletableFuture<?>> awaitingAck = new ConcurrentHashMap<>();
    private final String context;
    private final Map<String, ProcessorInstructionHandler> processorInstructionHandlers = new ConcurrentHashMap<>();
    private final Map<String, Supplier<EventProcessorInfo>> processorInfoSuppliers = new ConcurrentHashMap<String, Supplier<EventProcessorInfo>>();
    private final AtomicBoolean infoSupplierActive = new AtomicBoolean();

    public InstructionChannelImpl(ClientIdentification clientIdentification, String context,
                                  ScheduledExecutorService executor, AxonServerManagedChannel channel) {
        super(executor, channel);
        this.clientIdentification = clientIdentification;
        this.context = context;
        this.executor = executor;
        heartbeatMonitor = new HeartbeatMonitor(executor, this::sendHeartBeat, channel::forceReconnect);
        this.instructionHandlers.computeIfAbsent(PlatformOutboundInstruction.RequestCase.ACK, i -> new AckHandler());
        this.instructionHandlers.computeIfAbsent(PlatformOutboundInstruction.RequestCase.HEARTBEAT, i -> heartbeatMonitor::handleIncomingBeat);
        this.instructionHandlers.computeIfAbsent(PlatformOutboundInstruction.RequestCase.MERGE_EVENT_PROCESSOR_SEGMENT, i -> ProcessorInstructions.mergeHandler(processorInstructionHandlers));
        this.instructionHandlers.computeIfAbsent(PlatformOutboundInstruction.RequestCase.SPLIT_EVENT_PROCESSOR_SEGMENT, i -> ProcessorInstructions.splitHandler(processorInstructionHandlers));
        this.instructionHandlers.computeIfAbsent(PlatformOutboundInstruction.RequestCase.START_EVENT_PROCESSOR, i -> ProcessorInstructions.startHandler(processorInstructionHandlers));
        this.instructionHandlers.computeIfAbsent(PlatformOutboundInstruction.RequestCase.PAUSE_EVENT_PROCESSOR, i -> ProcessorInstructions.pauseHandler(processorInstructionHandlers));
        this.instructionHandlers.computeIfAbsent(PlatformOutboundInstruction.RequestCase.RELEASE_SEGMENT, i -> ProcessorInstructions.releaseSegmentHandler(processorInstructionHandlers));
        this.instructionHandlers.computeIfAbsent(PlatformOutboundInstruction.RequestCase.REQUEST_EVENT_PROCESSOR_INFO, i -> ProcessorInstructions.requestInfoHandler(processorInfoSuppliers));
    }

    @Override
    public synchronized void connect(ManagedChannel channel) {
        StreamObserver<PlatformInboundInstruction> existing = instructionDispatcher.get();
        if (existing != null) {
            logger.info("Not connecting - connection already present");
        } else {
            PlatformServiceGrpc.PlatformServiceStub platformServiceStub = PlatformServiceGrpc.newStub(channel);
            PlatformOutboundInstructionHandler responseObserver = new PlatformOutboundInstructionHandler(clientIdentification.getClientId(), 0, 0, e -> scheduleReconnect());
            logger.debug("Opening instruction stream");
            StreamObserver<PlatformInboundInstruction> instructionsForPlatform = platformServiceStub.openStream(responseObserver);
            StreamObserver<PlatformInboundInstruction> previous = instructionDispatcher.getAndSet(instructionsForPlatform);
            silently(previous, StreamObserver::onCompleted);

            try {
                logger.info("Connected instruction stream for context '{}'. Sending client identification", context);
                instructionsForPlatform.onNext(PlatformInboundInstruction.newBuilder().setRegister(clientIdentification).build());
                heartbeatMonitor.resume();
            } catch (Exception e) {
                instructionDispatcher.set(null);
                instructionsForPlatform.onError(e);
            }
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
    public Registration registerInstructionHandler(PlatformOutboundInstruction.RequestCase type, InstructionHandler handler) {
        instructionHandlers.put(type, handler);
        return () -> instructionHandlers.remove(type, handler);
    }

    @Override
    public Registration registerEventProcessor(String processorName, Supplier<EventProcessorInfo> infoSupplier, ProcessorInstructionHandler instructionHandler) {
        processorInstructionHandlers.put(processorName, instructionHandler);
        processorInfoSuppliers.put(processorName, infoSupplier);
        if (infoSupplierActive.compareAndSet(false, true)) {
            // first processor is registered
            sendScheduledProcessorInfo();

        }
        return () -> {
            processorInstructionHandlers.remove(processorName, instructionHandler);
            processorInfoSuppliers.remove(processorName, infoSupplier);
        };
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
            infoSupplies.forEach(info -> doIfNotNull(info.get(), this::sendProcessorInfo));
            executor.schedule(this::sendScheduledProcessorInfo, 500, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void enableHeartbeat(long interval, long timeout, TimeUnit timeUnit) {
        heartbeatMonitor.enableHeartbeat(interval, timeout, timeUnit);
    }

    @Override
    public void disableHeartbeat() {
        heartbeatMonitor.disableHeartbeat();
    }

    public CompletableFuture<Void> sendProcessorInfo(EventProcessorInfo processorInfo) {
        return sendInstruction(PlatformInboundInstruction.newBuilder().setEventProcessorInfo(processorInfo).build());
    }

    public CompletableFuture<Void> sendHeartBeat() {
        if (!isConnected()) {
            return CompletableFuture.completedFuture(null);
        }
        return sendInstruction(PlatformInboundInstruction.newBuilder().setInstructionId(UUID.randomUUID().toString()).setHeartbeat(Heartbeat.newBuilder()).build());
    }

    private CompletableFuture<Void> sendInstruction(PlatformInboundInstruction instruction) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        awaitingAck.put(instruction.getInstructionId(), result);
        logger.debug("Sending instruction: {}", instruction.getRequestCase().name());
        StreamObserver<PlatformInboundInstruction> dispatcher = instructionDispatcher.get();
        if (dispatcher == null) {
            result.completeExceptionally(new AxonServerException(ErrorCode.INSTRUCTION_EXECUTION_ERROR,
                                                                 "Unable to send instruction"));
        } else {
            dispatcher.onNext(instruction);
        } return result;
    }

    @Override
    public boolean isConnected() {
        return instructionDispatcher.get() != null;
    }

    private class PlatformOutboundInstructionHandler extends AbstractIncomingInstructionStream<PlatformOutboundInstruction, PlatformInboundInstruction> {

        public PlatformOutboundInstructionHandler(String clientId, int permits, int permitsBatch, Consumer<Throwable> disconnectHandler) {
            super(clientId, permits, permitsBatch, disconnectHandler);
        }

        @Override
        protected PlatformInboundInstruction buildAckMessage(InstructionAck ack) {
            return PlatformInboundInstruction.newBuilder().setAck(ack).build();
        }

        @Override
        protected String getInstructionId(PlatformOutboundInstruction value) {
            return value.getInstructionId();
        }

        @Override
        protected BiConsumer<PlatformOutboundInstruction, ReplyChannel<PlatformInboundInstruction>> getHandler(PlatformOutboundInstruction platformOutboundInstruction) {
            return instructionHandlers.get(platformOutboundInstruction.getRequestCase());
        }

        @Override
        protected boolean unregisterOutboundStream(StreamObserver<PlatformInboundInstruction> expected) {
            heartbeatMonitor.pause();
            return instructionDispatcher.compareAndSet(expected, null);
        }

        @Override
        protected PlatformInboundInstruction buildFlowControlMessage(FlowControl flowControl) {
            return null;
        }
    }

    private class AckHandler implements InstructionHandler {

        @Override
        public void accept(PlatformOutboundInstruction ackMessage, ReplyChannel<PlatformInboundInstruction> replyChannel) {
            String instructionId = ackMessage.getAck().getInstructionId();
            logger.info("Received ACK for {}", instructionId);
            CompletableFuture<?> handle = awaitingAck.remove(instructionId);
            if (handle != null) {
                handle.complete(null);
            }
        }
    }
}
