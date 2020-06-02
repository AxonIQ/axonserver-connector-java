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

import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;
import java.util.function.Consumer;


public abstract class AbstractIncomingInstructionStream<IN, OUT> extends FlowControlledStream<IN, OUT> {

    private static final InstructionAck NO_HANDLER_FOR_INSTRUCTION =
            InstructionAck.newBuilder().setSuccess(false)
                          .setError(ErrorMessage.newBuilder()
                                                .setErrorCode(ErrorCategory.UNSUPPORTED_INSTRUCTION.errorCode())
                                                .setMessage("No handler for instruction")
                                                .build())
                          .build();
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Consumer<Throwable> disconnectHandler;

    private StreamObserver<OUT> instructionsForPlatform;

    public AbstractIncomingInstructionStream(String clientId, int permits, int permitsBatch, Consumer<Throwable> disconnectHandler) {
        super(clientId, permits, permitsBatch);
        this.disconnectHandler = disconnectHandler;
    }

    @Override
    public void onNext(IN value) {
        BiConsumer<IN, ReplyChannel<OUT>> handler = getHandler(value);
        if (handler == null) {
            markConsumed();
            String instructionId = getInstructionId(value);
            if (instructionId != null && !instructionId.isEmpty()) {
                instructionsForPlatform.onNext(buildAckMessage(NO_HANDLER_FOR_INSTRUCTION));
            }
        } else {
            ForwardingReplyChannel<OUT> replyChannel = new ForwardingReplyChannel<>(getInstructionId(value), clientId(), instructionsForPlatform, this::buildAckMessage, this::markConsumed);
            handler.accept(value, replyChannel);
        }
    }

    protected abstract OUT buildAckMessage(InstructionAck ack);

    protected abstract String getInstructionId(IN value);

    protected abstract BiConsumer<IN, ReplyChannel<OUT>> getHandler(IN msgIn);

    @Override
    public void onCompleted() {
        logger.debug("Stream completed from server side");
        if (unregisterOutboundStream(instructionsForPlatform)) {
            instructionsForPlatform.onCompleted();
        }
    }

    @Override
    public void onError(Throwable t) {
        logger.debug("Error received", t);
        if (unregisterOutboundStream(instructionsForPlatform)) {
            logger.debug("Instruction stream disconnected. Scheduling reconnect");
            disconnectHandler.accept(t);
            instructionsForPlatform.onCompleted();
        }
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<OUT> requestStream) {
        super.beforeStart(requestStream);
        this.instructionsForPlatform = requestStream;
    }

    protected abstract boolean unregisterOutboundStream(StreamObserver<OUT> expected);

}
