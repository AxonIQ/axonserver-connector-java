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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link ReplyChannel} implementation that will trigger a given {@link Runnable action} when it is closed.
 *
 * @param <T> the type of messages flowing through this {@link ReplyChannel}
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 * @since 4.6.0
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
     * Instantiates this {@link ReplyChannel} with given {@code delegate} and {@code onClose} {@link Runnable} execution
     * to be executed once the channel is closed.
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
