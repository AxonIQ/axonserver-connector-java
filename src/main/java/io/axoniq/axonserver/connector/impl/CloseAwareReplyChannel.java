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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link ReplyChannel} that will trigger given action when is closed.
 *
 * @param <T> the type of messages flowing through this {@link ReplyChannel}
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 */
public class CloseAwareReplyChannel<T> implements ReplyChannel<T> {

    private final ReplyChannel<T> delegate;
    private final Runnable onClose;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Instantiates this {@link ReplyChannel} with given {@code delegate}.
     *
     * @param delegate the delegate
     */
    public CloseAwareReplyChannel(ReplyChannel<T> delegate) {
        this(delegate, () -> {
        });
    }

    /**
     * Instantiates this {@link ReplyChannel} with given {@code delegate} and {@code onClose} execution to be executed
     * once the channel is closed.
     *
     * @param delegate the delegate
     * @param onClose  to be executed when channel is closed
     */
    public CloseAwareReplyChannel(ReplyChannel<T> delegate, Runnable onClose) {
        this.delegate = delegate;
        this.onClose = () -> {
            closed.set(true);
            onClose.run();
        };
    }

    @Override
    public void send(T outboundMessage) {
        delegate.send(outboundMessage);
    }

    @Override
    public void sendLast(T outboundMessage) {
        delegate.sendLast(outboundMessage);
        onClose.run();
    }

    @Override
    public void sendAck() {
        delegate.sendAck();
    }

    @Override
    public void sendNack() {
        delegate.sendNack();
    }

    @Override
    public void sendNack(ErrorMessage errorMessage) {
        delegate.sendNack(errorMessage);
    }

    @Override
    public void complete() {
        delegate.complete();
        onClose.run();
    }

    @Override
    public void completeWithError(ErrorMessage errorMessage) {
        delegate.completeWithError(errorMessage);
        onClose.run();
    }

    @Override
    public void completeWithError(ErrorCategory errorCategory, String message) {
        delegate.completeWithError(errorCategory, message);
        onClose.run();
    }

    /**
     * Indicates whether this {@link ReplyChannel} is closed.
     *
     * @return {@code true} if closed, {@code false} otherwise
     */
    public boolean isClosed() {
        return closed.get();
    }
}
