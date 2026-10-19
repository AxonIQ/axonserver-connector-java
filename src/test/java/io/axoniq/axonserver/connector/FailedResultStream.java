/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link ResultStream} that throws {@link RuntimeException}s.
 * For unit test purpose.
 *
 * @author Sara Pellegrini
 */
public class FailedResultStream implements ResultStream<Long> {

    @Override
    public Long peek() {
        throw new RuntimeException();
    }

    @Override
    public Long nextIfAvailable() {
        throw new RuntimeException();
    }

    @Override
    public Long nextIfAvailable(long timeout, TimeUnit unit) {
        throw new RuntimeException();
    }

    @Override
    public Long next() {
        throw new RuntimeException();
    }

    @Override
    public void onAvailable(Runnable callback) {
        callback.run();
    }

    @Override
    public void close() {
        throw new RuntimeException();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public Optional<Throwable> getError() {
        return Optional.of(new RuntimeException());
    }
}
