package io.axoniq.axonserver.connector.impl;

import io.grpc.stub.ClientCallStreamObserver;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicBoolean;

public class SynchronizedRequestStream<T> extends ClientCallStreamObserver<T> {

    private final ClientCallStreamObserver<T> delegate;
    private final AtomicBoolean lock = new AtomicBoolean(false);

    public SynchronizedRequestStream(ClientCallStreamObserver<T> requestStream) {
        delegate = requestStream;
    }

    @Override
    public void cancel(@Nullable String message, @Nullable Throwable cause) {
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
    public void onError(Throwable t) {
        inLock(() -> delegate.onError(t));
    }

    @Override
    public void onCompleted() {
        inLock(delegate::onCompleted);
    }

    private void inLock(Runnable action) {
        while (!lock.compareAndSet(false, true)) {
            Thread.yield();
        }
        try {
            action.run();
        } finally {
            lock.set(false);
        }
    }
}
