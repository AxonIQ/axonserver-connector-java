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

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.InstructionHandler;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.InstructionResult;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Abstract implementation of a {@link FlowControlledStream} for incoming message from AxonServer.
 *
 * @param <IN>  the type of instructions received by this stream
 * @param <OUT> the type of instructions returned by this stream
 * @author Allard Buijze
 * @since 4.4
 */
public abstract class AbstractIncomingInstructionStream<IN, OUT> extends FlowControlledStream<IN, OUT> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final InstructionAck NO_HANDLER_FOR_INSTRUCTION =
            InstructionAck.newBuilder().setSuccess(false)
                          .setError(ErrorMessage.newBuilder()
                                                .setErrorCode(ErrorCategory.UNSUPPORTED_INSTRUCTION.errorCode())
                                                .setMessage("No handler for instruction")
                                                .build())
                          .build();

    private static final InstructionAck SUCCESS_ACK =
            InstructionAck.newBuilder().setSuccess(true).build();

    private final Consumer<Throwable> disconnectHandler;

    private final Consumer<CallStreamObserver<OUT>> beforeStartHandler;
    private final String clientId;
    private CallStreamObserver<OUT> instructionsForPlatform;

    /**
     * Construct an {@link AbstractIncomingInstructionStream}.
     *
     * @param clientId           the client identifier whom initiated this instruction stream
     * @param permits            the number of permits this stream should receive
     * @param permitsBatch       the number of permits to be consumed prior to requesting new permits
     * @param disconnectHandler  a {@link Consumer} of {@link Throwable} invoked when this stream errors out
     * @param beforeStartHandler the handler to invoke when the upstream connection is available. Note that the gRPC
     *                           call has not started yet at this point.
     */
    protected AbstractIncomingInstructionStream(String clientId,
                                                int permits,
                                                int permitsBatch,
                                                Consumer<Throwable> disconnectHandler,
                                                Consumer<CallStreamObserver<OUT>> beforeStartHandler) {
        super(clientId, permits, permitsBatch);
        this.clientId = clientId;
        this.disconnectHandler = disconnectHandler;
        this.beforeStartHandler = beforeStartHandler;
    }

    @Override
    public void onNext(IN value) {
        InstructionHandler<IN, OUT> handler = getHandler(value);
        String instructionId = getInstructionId(value);
        if (handler == null) {
            logger.debug("Unsupported instruction received: {}", value);
            markConsumed();
            if (instructionId != null && !instructionId.isEmpty()) {
                instructionsForPlatform.onNext(buildAckMessage(NO_HANDLER_FOR_INSTRUCTION));
            }
        } else {
            if (instructionId != null && !instructionId.isEmpty()) {
                instructionsForPlatform.onNext(buildAckMessage(SUCCESS_ACK));
            }
            ForwardingReplyChannel<OUT> replyChannel = new ForwardingReplyChannel<>(getInstructionId(value),
                                                                                    clientId,
                                                                                    instructionsForPlatform,
                                                                                    this::buildResultMessage,
                                                                                    this::markConsumed);
            handler.handle(value, replyChannel);
        }
    }

    /**
     * Builds a stream specific acknowledgment message of type {@code OUT} based on the given {@code ack}.
     *
     * @param ack the {@link InstructionAck} to base the stream specific acknowledgement on
     * @return a stream specific acknowledgment message of type {@code OUT} based on the given {@code ack}
     */
    protected abstract OUT buildAckMessage(InstructionAck ack);

    /**
     * Builds a stream specific result message of type {@code OUT} based on the given {@code result}. If the
     * {@link Optional} is empty, no instruction result will be sent. By default, this method returns an empty
     * {@link Optional}. If the concrete class supports the {@link InstructionResult} it should override this method.
     *
     * @param result the {@link InstructionResult} to base the stream specific acknowledgement on
     * @return a stream specific acknowledgment message of type {@code OUT} based on the given {@code result}
     */
    protected Optional<OUT> buildResultMessage(InstructionResult result) {
        return Optional.empty();
    }

    /**
     * Returns the instruction identifier of the given {@code instruction}.
     *
     * @param instruction the instruction of type {@code IN} to retrieve the instruction identifier from
     * @return the instruction identifier of the given {@code instruction}
     */
    protected abstract String getInstructionId(IN instruction);

    /**
     * Retrieves an {@link InstructionHandler} capable of handling the given {@code msgIn}.
     *
     * @param msgIn the instruction message of type {@code IN} to retrieve an {@link InstructionHandler} on
     * @return an {@link InstructionHandler} capable of handling the given {@code msgIn}
     */
    protected abstract InstructionHandler<IN, OUT> getHandler(IN msgIn);

    @Override
    public void onCompleted() {
        logger.debug("Stream completed from server side");
        if (unregisterOutboundStream(instructionsForPlatform)) {
            logger.debug("Instruction stream disconnected. Scheduling reconnect");
            Throwable t = new StreamUnexpectedlyCompletedException("Stream unexpectedly completed by server");
            disconnectHandler.accept(t);
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
        SynchronizedRequestStream<OUT> synchronizedRequestStream = new SynchronizedRequestStream<>(requestStream);
        super.beforeStart(synchronizedRequestStream);
        this.instructionsForPlatform = synchronizedRequestStream;
        this.beforeStartHandler.accept(getInstructionsForPlatform());
    }

    /**
     * Return the {@link StreamObserver} of type {@code OUT} serving as the outbound instruction channel.
     *
     * @return the {@link StreamObserver} of type {@code OUT} serving as the outbound instruction channel
     */
    public ClientCallStreamObserver<OUT> getInstructionsForPlatform() {
        return outboundStream();
    }

    /**
     * Unregisters this stream's outbound stream, granted that it matches the given {@code expected} {@link
     * StreamObserver}. Will return {@code true} if they matched and {@code false} otherwise.
     *
     * @param expected the expected {@link StreamObserver} to be unregistered
     *
     * @return {@code true} if the outbound stream was successfully unregistered, {@code false} otherwise
     */
    protected abstract boolean unregisterOutboundStream(CallStreamObserver<OUT> expected);
}
