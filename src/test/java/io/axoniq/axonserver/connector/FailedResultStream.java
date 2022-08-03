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
