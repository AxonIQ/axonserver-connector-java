/*
 * Copyright (c) 2022. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
