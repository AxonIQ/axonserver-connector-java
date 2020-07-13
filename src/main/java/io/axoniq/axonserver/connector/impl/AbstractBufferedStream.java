package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ResultStream;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractBufferedStream<T, R> extends FlowControlledBuffer<T, R> implements ResultStream<T> {

    public static final Runnable NO_OP = () -> {
    };

    private final AtomicReference<Runnable> onAvailableCallback = new AtomicReference<>(NO_OP);

    public AbstractBufferedStream(String clientId, int bufferSize, int refillBatch) {
        super(clientId, bufferSize, refillBatch);
    }

    @Override
    public T next() throws InterruptedException {
        return take();
    }

    @Override
    public T nextIfAvailable() {
        return tryTakeNow();
    }

    @Override
    public T nextIfAvailable(long timeout, TimeUnit unit) throws InterruptedException {
        return tryTake(timeout, unit);
    }

    @Override
    public void onNext(T value) {
        super.onNext(value);
        onAvailableCallback.get().run();
    }

    @Override
    public void onError(Throwable t) {
        super.onError(t);
        onAvailableCallback.get().run();
    }

    @Override
    public void onCompleted() {
        super.onCompleted();
        onAvailableCallback.get().run();
    }

    @Override
    public T peek() {
        return super.peek();
    }

    @Override
    public void onAvailable(Runnable callback) {
        if (callback == null) {
            onAvailableCallback.set(NO_OP);
        } else {
            onAvailableCallback.set(callback);
            if (peek() != null) {
                callback.run();
            }
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
