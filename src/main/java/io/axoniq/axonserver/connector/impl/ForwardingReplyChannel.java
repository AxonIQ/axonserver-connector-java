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

import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * A {@link ReplyChannel} implementation which forwards {@link #send(Object)}, {@link #sendAck()} and {@link
 * #sendNack(ErrorMessage)} operations through to a {@link StreamObserver}.
 *
 * @param <T> the message type forwarded by this {@link ReplyChannel} to the given {@link StreamObserver}
 */
public class ForwardingReplyChannel<T> implements ReplyChannel<T> {

    private final AtomicBoolean ackSent = new AtomicBoolean(false);
    private final String instructionId;
    private final String clientId;
    private final StreamObserver<T> stream;
    private final Function<InstructionAck, T> ackBuilder;
    private final Runnable onConsumed;
    private final AtomicBoolean completed = new AtomicBoolean();

    /**
     * Construct a {@link ForwardingReplyChannel} to forward replies to the given {@code stream}.
     *
     * @param instructionId the instruction identifier used to send ACK or NACK messages. If the given instruction
     *                      identifier is {@code null} or empty, no ACK/NACK will be send
     * @param clientId      the client identifier used to define the error location upon a {@link
     *                      #completeWithError(ErrorCategory, String)} invocation
     * @param stream        the {@link StreamObserver} to forward replies of this {@link ReplyChannel} on
     * @param ackBuilder    the builder function used to construct the {@link InstructionAck} message, used for both a
     *                      {@link #sendAck()} and {@link #sendNack()}
     * @param onComplete    operation to perform when this this {@link ReplyChannel} is completed, both successfully and
     *                      exceptionally
     */
    public ForwardingReplyChannel(String instructionId,
                                  String clientId,
                                  StreamObserver<T> stream,
                                  Function<InstructionAck, T> ackBuilder,
                                  Runnable onComplete) {
        this.instructionId = instructionId;
        this.clientId = clientId;
        this.stream = stream;
        this.ackBuilder = ackBuilder;
        this.onConsumed = onComplete;
    }

    @Override
    public void send(T outboundMessage) {
        stream.onNext(outboundMessage);
    }

    @Override
    public void sendAck() {
        if (instructionId != null && !instructionId.isEmpty() && ackSent.compareAndSet(false, true)) {
            stream.onNext(ackBuilder.apply(InstructionAck.newBuilder()
                                                         .setInstructionId(instructionId)
                                                         .setSuccess(true)
                                                         .build()));
        }
    }

    @Override
    public void complete() {
        sendAck();
        markConsumed();
    }

    @Override
    public void completeWithError(ErrorMessage errorMessage) {
        if (errorMessage.getLocation().isEmpty()) {
            errorMessage = errorMessage.toBuilder()
                                       .setLocation(clientId)
                                       .build();
        }
        sendNack(errorMessage);
        markConsumed();
    }

    @Override
    public void sendNack(ErrorMessage errorMessage) {
        if (instructionId != null && !instructionId.isEmpty() && ackSent.compareAndSet(false, true)) {
            InstructionAck nack =
                    InstructionAck.newBuilder()
                                  .setInstructionId(instructionId)
                                  .setError(errorMessage == null ? ErrorMessage.getDefaultInstance() : errorMessage)
                                  .setSuccess(false)
                                  .build();
            stream.onNext(ackBuilder.apply(nack));
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
