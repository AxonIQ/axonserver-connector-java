/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl.buffer;

import io.axoniq.axonserver.connector.FlowControl;
import io.axoniq.axonserver.connector.impl.CloseableReadonlyBuffer;
import io.axoniq.axonserver.connector.impl.DisposableReadonlyBuffer;
import io.axoniq.axonserver.grpc.ErrorMessage;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connects a {@link FlowControl} instance with a {@link DisposableReadonlyBuffer} assuming that {@link
 * FlowControl#request(long) requesting} from the flow control will trigger a replenishment of the buffer.
 *
 * @param <T> the type of messages this buffer deals with
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 * @since 4.6.0
 */
public class FlowControlledDisposableReadonlyBuffer<T> implements DisposableReadonlyBuffer<T> {

    private final FlowControl flowControl;
    private final CloseableReadonlyBuffer<T> buffer;
    private final AtomicBoolean started = new AtomicBoolean();

    /**
     * Instantiates this buffer with the given {@code flowControl} and {@code buffer}.
     *
     * @param flowControl used for {@code buffer} replenishment
     * @param buffer      used for message retrieval on {@link FlowControl#request(long)}
     */
    public FlowControlledDisposableReadonlyBuffer(FlowControl flowControl, CloseableReadonlyBuffer<T> buffer) {
        this.flowControl = flowControl;
        this.buffer = buffer;
    }

    @Override
    public Optional<T> poll() {
        replenishIfNotStarted();
        Optional<T> element = buffer.poll();
        element.ifPresent(e -> markConsumed());
        return element;
    }

    @Override
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    @Override
    public int capacity() {
        return buffer.capacity();
    }

    @Override
    public void onAvailable(Runnable onAvailable) {
        buffer.onAvailable(onAvailable);
    }

    @Override
    public void dispose() {
        flowControl.cancel();
    }

    @Override
    public boolean closed() {
        return buffer.closed();
    }

    @Override
    public Optional<ErrorMessage> error() {
        return buffer.error();
    }

    private void replenishIfNotStarted() {
        if (started.compareAndSet(false, true)) {
            flowControl.request(buffer.capacity());
        }
    }

    private void markConsumed() {
        if (!closed()) {
            flowControl.request(1);
        }
    }
}
