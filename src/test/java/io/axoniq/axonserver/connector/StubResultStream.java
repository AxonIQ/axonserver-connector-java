/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fake implementation of {@link ResultStream} that returns a certain number of {@link Long} elements.
 * For unit test purpose.
 *
 * @author Sara Pellegrini
 */
public class StubResultStream implements ResultStream<Long> {

    private final AtomicLong e;

    public StubResultStream(long elements) {
        this.e = new AtomicLong(elements);
    }

    @Override
    public Long peek() {
        return e.get() == 0L ? null : e.get();
    }

    @Override
    public Long nextIfAvailable() {
        return e.get() == 0L ? null : e.getAndDecrement();
    }

    @Override
    public Long nextIfAvailable(long timeout, TimeUnit unit) {
        return nextIfAvailable();
    }

    @Override
    public Long next() {
        return nextIfAvailable();
    }

    @Override
    public void onAvailable(Runnable callback) {
        callback.run();
    }

    @Override
    public void close() {
        e.set(0L);
    }

    @Override
    public boolean isClosed() {
        return e.get() == 0L;
    }

    @Override
    public Optional<Throwable> getError() {
        return Optional.empty();
    }
}
