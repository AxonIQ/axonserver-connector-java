package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ResultStream;

import java.util.concurrent.TimeUnit;

public abstract class AbstractBufferedStream<T, R> extends FlowControlledBuffer<T, R> implements ResultStream<T> {

    private volatile Runnable onAvailableCallback = () -> {};

    public AbstractBufferedStream(String clientId, int bufferSize, int refillBatch) {
        super(clientId, bufferSize, refillBatch);
    }

    @Override
    public T next() throws InterruptedException, StreamClosedException {
        return take();
    }

    @Override
    public T nextIfAvailable() {
        return tryTakeNow();
    }

    @Override
    public T nextIfAvailable(int timeout, TimeUnit unit) throws InterruptedException {
        return tryTake(timeout, unit);
    }

    @Override
    public void onNext(T value) {
        super.onNext(value);
        onAvailableCallback.run();
    }

    @Override
    public void onError(Throwable t) {
        super.onError(t);
        onAvailableCallback.run();
    }

    @Override
    public void onCompleted() {
        super.onCompleted();
        onAvailableCallback.run();
    }

    @Override
    public T peek() {
        return super.peek();
    }

    @Override
    public void onAvailable(Runnable callback) {
        if (callback == null) {
            onAvailableCallback = () -> {};
        } else {
            onAvailableCallback = callback;
        }
        if (peek() != null) {
            onAvailableCallback.run();
        }
    }

    @Override
    public boolean isClosed() {
        return super.isClosed();
    }

    @Override
    public void close() {
        outboundStream().onCompleted();
    }

}
