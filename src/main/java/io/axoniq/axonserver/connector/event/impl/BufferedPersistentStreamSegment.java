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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

public class BufferedPersistentStreamSegment
        implements PersistentStreamSegment {
    private static final EventWithToken TERMINAL_MESSAGE = EventWithToken.newBuilder().setToken(-1729).build();

    private static final Runnable NO_OP = () -> {
    };

    private final Set<Runnable> onSegmentClosedCallbacks = new CopyOnWriteArraySet<>();
    private final BlockingQueue<EventWithToken> buffer = new LinkedBlockingQueue<>();
    private final AtomicReference<Throwable> errorResult = new AtomicReference<>();
    private final AtomicReference<Runnable> onAvailableCallback = new AtomicReference<>(NO_OP);

    private final int segment;
    private final LongConsumer progressCallback;

    public BufferedPersistentStreamSegment(int segment, LongConsumer progressCallback) {
        this.segment = segment;
        this.progressCallback = progressCallback;
    }

    @Override
    public EventWithToken peek() {
        return validate(buffer.peek(), false);
    }

    @Override
    public EventWithToken nextIfAvailable() {
        return validate(buffer.poll(), false);
    }

    @Override
    public EventWithToken nextIfAvailable(long timeout, TimeUnit unit) throws InterruptedException {
        return validate(buffer.poll(timeout, unit), false);
    }

    @Override
    public EventWithToken next() throws InterruptedException {
        return validate(buffer.take(), false);
    }

    @Override
    public void onAvailable(Runnable callback) {
        if (callback == null) {
            onAvailableCallback.set(NO_OP);
        } else {
            onAvailableCallback.set(callback);
            if (validate(peek(), true) != null) {
                callback.run();
            }
        }
    }


    private EventWithToken validate(EventWithToken peek, boolean nullOnTerminal) {
        if (TERMINAL_MESSAGE.equals(peek)) {
            if (buffer.isEmpty()) {
                // just to make sure there is always a TERMINAL entry left in a terminated buffer
                buffer.offer(TERMINAL_MESSAGE);
            }
            if (nullOnTerminal) {
                return null;
            }
            throw new StreamClosedException(errorResult.get());
        }
        return peek;
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public boolean isClosed() {
        return TERMINAL_MESSAGE.equals(buffer.peek());
    }

    @Override
    public Optional<Throwable> getError() {
        return Optional.ofNullable(errorResult.get());
    }

    @Override
    public void onSegmentClosed(Runnable callback) {
        onSegmentClosedCallbacks.add(callback);
    }

    public void addNext(EventWithToken streamSignal) {
        buffer.add(streamSignal);
        onAvailableCallback.get().run();
    }

    public void markError(Throwable throwable) {
        errorResult.set(throwable);
        markCompleted();
    }

    public void markCompleted() {
        buffer.offer(TERMINAL_MESSAGE);
        onSegmentClosedCallbacks.forEach(Runnable::run);
        onAvailableCallback.get().run();
    }

    @Override
    public void acknowledge(long token) {
        if (!isClosed()) {
            progressCallback.accept(token);
        }
    }

    @Override
    public int segment() {
        return segment;
    }
}
