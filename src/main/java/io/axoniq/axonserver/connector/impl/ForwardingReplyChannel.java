/*
 * Copyright (c) 2022. AxonIQ
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
import io.axoniq.axonserver.grpc.InstructionResult;
import io.grpc.stub.StreamObserver;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * A {@link ReplyChannel} implementation which forwards {@link #send(Object)}, {@link #sendSuccessResult()} and {@link
 * #sendFailureResult(ErrorMessage)} operations through to a {@link StreamObserver}.
 *
 * @param <T> the message type forwarded by this {@link ReplyChannel} to the given {@link StreamObserver}
 */
public class ForwardingReplyChannel<T> implements ReplyChannel<T> {

    private final AtomicBoolean resultSent = new AtomicBoolean(false);
    private final String instructionId;
    private final String clientId;
    private final StreamObserver<T> stream;
    private final Function<InstructionResult, Optional<T>> resultBuilder;
    private final Runnable onConsumed;
    private final AtomicBoolean completed = new AtomicBoolean();

    /**
     * Construct a {@link ForwardingReplyChannel} to forward replies to the given {@code stream}.
     *
     * @param instructionId the instruction identifier used to send Result Messages. If the given instruction
     *                      identifier is {@code null} or empty, no result will be sent
     * @param clientId      the client identifier used to define the error location upon a {@link
     *                      #completeWithError(ErrorCategory, String)} invocation
     * @param stream        the {@link StreamObserver} to forward replies of this {@link ReplyChannel} on
     * @param resultBuilder the builder function used to construct the {@link InstructionResult} message,
     *                      used for both a {@link #sendSuccessResult()} and {@link #sendFailureResult(ErrorMessage)}
     * @param onComplete    operation to perform when this {@link ReplyChannel} is completed, both successfully and
     *                      exceptionally
     */
    public ForwardingReplyChannel(String instructionId,
                                  String clientId,
                                  StreamObserver<T> stream,
                                  Function<InstructionResult, Optional<T>> resultBuilder,
                                  Runnable onComplete) {
        this.instructionId = instructionId;
        this.clientId = clientId;
        this.stream = stream;
        this.resultBuilder = resultBuilder;
        this.onConsumed = onComplete;
    }

    @Override
    public void send(T outboundMessage) {
        stream.onNext(outboundMessage);
    }

    /**
     * Sends a confirmation that the instruction has been executed with success.
     * If not explicitly sent, it will be sent once the {@link #complete()} method is invoked.
     * <p>
     * If the incoming instruction has no instruction ID, this method does nothing.
     */
    private void sendSuccessResult() {
        if (instructionId != null && !instructionId.isEmpty() && resultSent.compareAndSet(false, true)) {
            resultBuilder.apply(InstructionResult.newBuilder()
                                                 .setInstructionId(instructionId)
                                                 .setSuccess(true)
                                                 .build())
                         .ifPresent(stream::onNext);
        }
    }

    @Override
    public void complete() {
        sendSuccessResult();
        markConsumed();
    }

    @Override
    public void completeWithError(ErrorMessage errorMessage) {
        sendFailureResult(errorMessage);
        markConsumed();
    }

    /**
     * Sends a failed result, indicating that the incoming message could not be handled as expected, using
     * given {@code errorMessage} to describe the reason. If not explicitly sent, it will be sent once the {@link
     * #completeWithError(ErrorMessage)} or {@link #completeWithError(ErrorCategory, String)} methods are invoked. The
     * given {@code errorMessage} should provide sufficient information about the error.
     * <p>
     * If the incoming instruction has no instruction ID, this method does nothing.
     */
    private void sendFailureResult(ErrorMessage errorMessage) {
        if (instructionId != null && !instructionId.isEmpty() && resultSent.compareAndSet(false, true)) {
            InstructionResult failure =
                    InstructionResult.newBuilder()
                                     .setInstructionId(instructionId)
                                     .setError(errorMessage == null ? ErrorMessage.getDefaultInstance() : errorMessage)
                                     .setSuccess(false)
                                     .build();
            resultBuilder.apply(failure).ifPresent(stream::onNext);
        }
    }

    @Override
    public void completeWithError(ErrorCategory errorCategory, String message) {
        completeWithError(ErrorMessage.newBuilder()
                                      .setErrorCode(errorCategory.errorCode())
                                      .setLocation(clientId)
                                      .setMessage(message)
                                      .build());
    }

    private void markConsumed() {
        if (completed.compareAndSet(false, true)) {
            onConsumed.run();
        }
    }
}
