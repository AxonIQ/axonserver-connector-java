package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.event.PersistentStreamSegment;
import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

public class BufferedPersistentStreamSegment
        implements PersistentStreamSegment {

    private static final Runnable NO_OP = () -> {
    };

    private final Set<Runnable> onSegmentClosedCallbacks = new CopyOnWriteArraySet<>();
    private final BlockingQueue<EventWithToken> buffer = new LinkedBlockingQueue<>();
    private final AtomicReference<Throwable> errorResult = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicReference<Runnable> onAvailableCallback = new AtomicReference<>(NO_OP);

    private final int segment;
    private final LongConsumer progressCallback;

    public BufferedPersistentStreamSegment(int segment, LongConsumer progressCallback) {
        this.segment = segment;
        this.progressCallback = progressCallback;
    }

    @Override
    public EventWithToken peek() {
        checkClosed();
        return buffer.peek();
    }

    @Override
    public EventWithToken nextIfAvailable() {
        checkClosed();
        return buffer.poll();
    }

    @Override
    public EventWithToken nextIfAvailable(long timeout, TimeUnit unit) throws InterruptedException {
        checkClosed();
        return buffer.poll(timeout, unit);
    }

    @Override
    public EventWithToken next() throws InterruptedException {
        checkClosed();
        return buffer.take();
    }

    @Override
    public void onAvailable(Runnable callback) {
        checkClosed();
        if (callback == null) {
            onAvailableCallback.set(NO_OP);
        } else {
            onAvailableCallback.set(callback);
            if (isClosed() || peek() != null) {
                callback.run();
            }
        }
    }

    private void checkClosed() {
        if (closed.get()) {
            throw new StreamClosedException(errorResult.get());
        }
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public Optional<Throwable> getError() {
        return Optional.ofNullable(errorResult.get());
    }

    @Override
    public void onSegmentClosed(Runnable callback) {
        onSegmentClosedCallbacks.add(callback);
    }

    public void onNext(EventWithToken streamSignal) {
        buffer.add(streamSignal);
        onAvailableCallback.get().run();
    }

    public void onError(Throwable throwable) {
        errorResult.set(throwable);
        onCompleted();
    }

    public void onCompleted() {
        closed.set(true);
        onSegmentClosedCallbacks.forEach(Runnable::run);
        onAvailableCallback.get().run();
    }

    @Override
    public void acknowledge(long token) {
        if (!closed.get()) {
            progressCallback.accept(token);
        }
    }

    @Override
    public int segment() {
        return segment;
    }
}
