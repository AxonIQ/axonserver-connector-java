/*
 * Copyright (c) 2020-2022. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * A reply channel that uses a given {@code buffer} to buffer sends, completes and completes with error. {@code ack}s
 * and {@code nack}s are delegated via given {@code delegate}.
 *
 * @param <T> the type of messages this {@link ReplyChannel} deals with
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 */
public class BufferingReplyChannel<T> implements ReplyChannel<T> {

    private final ReplyChannel<T> delegate;
    private final CloseableBuffer<T> buffer;

    /**
     * Instantiates this {@link BufferingReplyChannel} with given {@code delegate} and {@code buffer}.
     *
     * @param delegate used to delegate {@code ack}s and {@code nack}s
     * @param buffer   used to buffer sends, completes and completes with error
     */
    public BufferingReplyChannel(ReplyChannel<T> delegate, CloseableBuffer<T> buffer) {
        this.delegate = delegate;
        this.buffer = buffer;
    }

    @Override
    public void send(T outboundMessage) {
        buffer.put(outboundMessage);
    }

    @Override
    public void sendAck() {
        delegate.sendAck();
    }

    @Override
    public void sendNack(ErrorMessage errorMessage) {
        delegate.sendNack(errorMessage);
    }

    @Override
    public void complete() {
        buffer.close();
    }

    @Override
    public void completeWithError(ErrorMessage errorMessage) {
        buffer.closeExceptionally(errorMessage);
    }

    @Override
    public void completeWithError(ErrorCategory errorCategory, String message) {
        ErrorMessage error = ErrorMessage.newBuilder()
                                         .setErrorCode(errorCategory.errorCode())
                                         .setMessage(message)
                                         .build();
        buffer.closeExceptionally(error);
    }
}
