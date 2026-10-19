/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.grpc.ErrorMessage;

/**
 * A {@link ReplyChannel} implementation that uses a given {@link CloseableBuffer buffer} to buffer send, complete and
 * complete-with-error. {@code ACK}s and {@code NACK}s are delegated via {@code delegate ReplyChannel}.
 *
 * @param <T> the type of messages this {@link ReplyChannel} deals with
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 * @since 4.6.0
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
