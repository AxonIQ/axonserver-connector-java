/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.grpc.stub.ClientCallStreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lock-based synchronized implementation of a {@link ClientCallStreamObserver}. Acts as a wrapper of another {@code
 * ClientCallStreamObserver}, to which all of operations will be delegated, adding synchronization logic to {@link
 * #onNext(Object)}, {@link #onCompleted()} and {@link #onError(Throwable)}.
 *
 * @param <T> the type of value returned by this stream
 */
public class SynchronizedRequestStream<T> extends ClientCallStreamObserver<T> {

    private final ClientCallStreamObserver<T> delegate;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean halfClosed = new AtomicBoolean(false);

    /**
     * Instantiate a {@link SynchronizedRequestStream}, delegating all operations to the given {@code requestStream}
     *
     * @param requestStream the {@link ClientCallStreamObserver} to delegate method invocations to
     */
    public SynchronizedRequestStream(ClientCallStreamObserver<T> requestStream) {
        delegate = requestStream;
    }

    @Override
    public void cancel(String message, Throwable cause) {
        halfClosed.set(true);
        delegate.cancel(message, cause);
    }

    @Override
    public boolean isReady() {
        return delegate.isReady();
    }

    @Override
    public void setOnReadyHandler(Runnable onReadyHandler) {
        delegate.setOnReadyHandler(onReadyHandler);
    }

    @Override
    public void disableAutoInboundFlowControl() {
        delegate.disableAutoInboundFlowControl();
    }

    @Override
    public void request(int count) {
        delegate.request(count);
    }

    @Override
    public void setMessageCompression(boolean enable) {
        delegate.setMessageCompression(enable);
    }

    @Override
    public void onNext(T value) {
        inLock(() -> delegate.onNext(value));
    }

    @Override
    public void disableAutoRequestWithInitial(int request) {
        delegate.disableAutoRequestWithInitial(request);
    }

    @Override
    public void onError(Throwable t) {
        inLock(() -> {
            delegate.onError(t);
            halfClosed.set(true);
        });
    }

    @Override
    public void onCompleted() {
        inLock(() -> {
            delegate.onCompleted();
            halfClosed.set(true);
        });
    }

    private void inLock(Runnable action) {
        try {
            lock.lock();
            if (!halfClosed.get()) {
                action.run();
            }
        } finally {
            lock.unlock();
        }
    }
}
