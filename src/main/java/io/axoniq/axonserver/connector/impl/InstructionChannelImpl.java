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

import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.instruction.InstructionChannel;
import io.axoniq.axonserver.connector.instruction.InstructionHandler;
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.silently;

public class InstructionChannelImpl extends AbstractAxonServerChannel implements InstructionChannel {

    private static final Logger logger = LoggerFactory.getLogger(InstructionChannel.class);
    private final ClientIdentification clientIdentification;
    private final ScheduledExecutorService executor;
    private final AtomicReference<StreamObserver<PlatformInboundInstruction>> instructionDispatcher = new AtomicReference<>();
    private final Map<PlatformOutboundInstruction.RequestCase, BiConsumer<PlatformOutboundInstruction, ReplyChannel<PlatformInboundInstruction>>> instructionHandlers = new HashMap<>();
    private final HeartbeatMonitor heartbeatMonitor;

    public InstructionChannelImpl(ClientIdentification clientIdentification,
                                  ScheduledExecutorService executor, AxonServerManagedChannel channel) {
        super(executor, channel);
        this.clientIdentification = clientIdentification;
        this.executor = executor;
        this.instructionHandlers.computeIfAbsent(PlatformOutboundInstruction.RequestCase.ACK, i -> new AckHandler());
        heartbeatMonitor = new HeartbeatMonitor(executor, this::sendHeartBeat, channel::forceReconnect);
    }

    @Override
    public synchronized void connect(ManagedChannel channel) {
        StreamObserver<PlatformInboundInstruction> existing = instructionDispatcher.get();
        if (existing != null) {
            logger.info("Not connecting - connection already present");
        } else {
            PlatformServiceGrpc.PlatformServiceStub platformServiceStub = PlatformServiceGrpc.newStub(channel);
            PlatformOutboundInstructionHandler responseObserver = new PlatformOutboundInstructionHandler(clientIdentification.getClientId(), 0, 0, e -> scheduleReconnect());
            logger.info("Opening instruction stream");
            StreamObserver<PlatformInboundInstruction> instructionsForPlatform = platformServiceStub.openStream(responseObserver);
            StreamObserver<PlatformInboundInstruction> previous = instructionDispatcher.getAndSet(instructionsForPlatform);
            silently(previous, StreamObserver::onCompleted);

            logger.info("Connected instruction stream. Sending client identification");
            try {
                instructionsForPlatform.onNext(PlatformInboundInstruction.newBuilder().setRegister(clientIdentification).build());
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
        return sendInstruction(PlatformInboundInstruction.newBuilder().setHeartbeat(Heartbeat.newBuilder().build()).build());
    }

    private CompletableFuture<Void> sendInstruction(PlatformInboundInstruction instruction) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        // TODO - Provide support for sending instructions
        result.completeExceptionally(new UnsupportedOperationException("Sending instructions is not supported yet"));
        return result;
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
        protected boolean replaceOutBoundStream(StreamObserver<PlatformInboundInstruction> expected, StreamObserver<PlatformInboundInstruction> replaceBy) {
            return instructionDispatcher.compareAndSet(expected, replaceBy);
        }

        @Override
        protected PlatformInboundInstruction buildFlowControlMessage(FlowControl flowControl) {
            return null;
        }
    }

    private class AckHandler implements BiConsumer<PlatformOutboundInstruction, ReplyChannel<PlatformInboundInstruction>> {

        @Override
        public void accept(PlatformOutboundInstruction ackMessage, ReplyChannel<PlatformInboundInstruction> replyChannel) {
            // TODO: Check if a result handler was registered for the incoming ACK and call it.
        }
    }
}
