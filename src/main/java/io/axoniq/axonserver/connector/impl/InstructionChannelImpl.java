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

import io.axoniq.axonserver.connector.ErrorCode;
import io.axoniq.axonserver.connector.InstructionChannel;
import io.axoniq.axonserver.connector.InstructionHandler;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformServiceGrpc;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.silently;

public class InstructionChannelImpl extends AbstractAxonServerChannel implements InstructionChannel {

    private static final Logger logger = LoggerFactory.getLogger(InstructionChannel.class);
    private final ClientIdentification clientIdentification;
    private final AtomicReference<StreamObserver<PlatformInboundInstruction>> instructionDispatcher = new AtomicReference<>();
    private final InstructionHandler unknownInstructionHandler;
    private final Map<PlatformOutboundInstruction.RequestCase, Set<InstructionHandler>> instructionHandlers = new HashMap<>();

    public InstructionChannelImpl(ClientIdentification clientIdentification) {
        this.clientIdentification = clientIdentification;
        this.unknownInstructionHandler = new UnknownInstructionHandler(clientIdentification.getClientId());
        this.instructionHandlers.computeIfAbsent(PlatformOutboundInstruction.RequestCase.ACK, r -> new CopyOnWriteArraySet<>())
                                .add(new AckHandler());
    }

    @Override
    public synchronized void connect(ManagedChannel channel) {
        PlatformServiceGrpc.PlatformServiceStub platformServiceStub = PlatformServiceGrpc.newStub(channel);
        StreamObserver<PlatformInboundInstruction> existing = instructionDispatcher.get();
        if (existing == null) {
            logger.info("Connecting with ChannelState " + channel.getState(false).name());
            ConnectivityState state = channel.getState(true);
            logger.info("Connecting with ChannelState " + state.name());

            PlatformOutboundInstructionHandler responseObserver = new PlatformOutboundInstructionHandler();
            logger.info("Opening instruction stream");
            StreamObserver<PlatformInboundInstruction> instructionsForPlatform = platformServiceStub.openStream(responseObserver);
            StreamObserver<PlatformInboundInstruction> previous = instructionDispatcher.getAndSet(instructionsForPlatform);
            silently(previous, StreamObserver::onCompleted);
            responseObserver.registerDispatcher(instructionsForPlatform);

            StreamObserver<PlatformInboundInstruction> connection = instructionDispatcher.get();
            if (connection != null) {
                logger.info("Connected instruction stream");
                connection.onNext(PlatformInboundInstruction.newBuilder().setRegister(clientIdentification).build());
            } else {
                logger.info("Connection failed");
            }
        } else {
            logger.info("Not connecting - connection already present");
        }
    }

    @Override
    public void disconnect() {
        StreamObserver<PlatformInboundInstruction> dispatcher = instructionDispatcher.getAndSet(null);
        if (dispatcher != null) {
            dispatcher.onCompleted();
        }
    }

    @Override public Runnable registerInstructionHandler(PlatformOutboundInstruction.RequestCase type, InstructionHandler handler) {
        instructionHandlers.computeIfAbsent(type, t -> new CopyOnWriteArraySet<>())
                           .add(handler);
        return () -> instructionHandlers.getOrDefault(type, Collections.emptySet()).remove(handler);
    }

    @Override
    public boolean isConnected() {
        return instructionDispatcher.get() != null;
    }

    private static class UnknownInstructionHandler implements InstructionHandler {

        private final InstructionAck.Builder NO_ACK;

        private UnknownInstructionHandler(String clientId) {
            ErrorMessage.Builder error = ErrorMessage.newBuilder()
                                                     .setErrorCode(ErrorCode.UNSUPPORTED_INSTRUCTION.errorCode())
                                                     .setMessage("Unsupported instruction")
                                                     .setLocation(clientId);
            NO_ACK = InstructionAck.newBuilder()
                                   .setSuccess(false)
                                   .setError(error);
        }

        @Override
        public void handle(PlatformOutboundInstruction value, Consumer<PlatformInboundInstruction> replyStream) {
            logger.warn("Received unsupported instruction: {} ({})", value.getRequestCase().name(), value.getRequestCase().getNumber());
            if (!"".equals(value.getInstructionId())) {
                replyStream.accept(PlatformInboundInstruction.newBuilder()
                                                             .setAck(NO_ACK)
                                                             .setInstructionId(value.getInstructionId())
                                                             .build());
            }
        }
    }

    private class PlatformOutboundInstructionHandler implements StreamObserver<PlatformOutboundInstruction> {

        private StreamObserver<PlatformInboundInstruction> instructionsForPlatform;
        private volatile boolean closed;

        @Override
        public void onNext(PlatformOutboundInstruction value) {
            instructionHandlers.getOrDefault(value.getRequestCase(),
                                             Collections.singleton(unknownInstructionHandler))
                               .forEach(h -> h.handle(value, instructionsForPlatform::onNext));

        }

        @Override
        public void onError(Throwable t) {
            closed = true;
            logger.info("Received error. Attempting to close " + instructionsForPlatform);
            if (instructionDispatcher.compareAndSet(instructionsForPlatform, null)) {
                logger.warn("Received error on instruction channel.", t);
                try {
                    instructionsForPlatform.onCompleted();
                } catch (Exception e) {
                    // we did our best
                }
            }
        }

        @Override
        public void onCompleted() {
            logger.info("Stream completed from server side");
            closed = true;
            if (instructionDispatcher.compareAndSet(instructionsForPlatform, null)) {
                instructionsForPlatform.onCompleted();
                InstructionChannelImpl.this.disconnect();
            }
        }

        public void registerDispatcher(StreamObserver<PlatformInboundInstruction> instructionsForPlatform) {
            this.instructionsForPlatform = instructionsForPlatform;
            if (closed) {
                if (instructionDispatcher.compareAndSet(instructionsForPlatform, null)) {
                    logger.warn("Instruction channel failed to connect.");
                    instructionsForPlatform.onCompleted();
                }
            }
        }
    }

    private class AckHandler implements InstructionHandler {
        @Override
        public void handle(PlatformOutboundInstruction value, Consumer<PlatformInboundInstruction> replyStream) {
            // TODO: Check if a result handler was registered for the incoming ACK and call it.
        }
    }
}
