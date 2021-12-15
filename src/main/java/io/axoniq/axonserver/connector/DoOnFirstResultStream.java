/*
 * Copyright (c) 2020-2021. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Implementation of {@link ResultStream} that accepts {@code doOnFirstCallback} to invoke it when the very first
 * element is consumed/peeked. Apart from that addition everything is delegated.
 *
 * @param <T> the result type
 */
public class DoOnFirstResultStream<T> implements ResultStream<T> {

    private final ResultStream<T> delegate;
    private final AtomicBoolean firstConsumed = new AtomicBoolean();
    private final Consumer<T> doOnFirstCallback;

    /**
     * Constructs this {@link DoOnFirstResultStream} with the given {@code delegate} and {@code doOnFirstCallback} to be
     * invoked when the very first element is consumed/peeked.
     *
     * @param delegate          the {@link ResultStream} delegate
     * @param doOnFirstCallback a callback to be invoked when the very first element is consumed/peeked
     */
    public DoOnFirstResultStream(ResultStream<T> delegate, Consumer<T> doOnFirstCallback) {
        this.delegate = delegate;
        this.doOnFirstCallback = doOnFirstCallback;
    }

    @Override
    public T peek() {
        return invokeOnFirst(delegate.peek());
    }

    @Override
    public T nextIfAvailable() {
        return invokeOnFirst(delegate.nextIfAvailable());
    }

    @Override
    public T nextIfAvailable(long timeout, TimeUnit unit) throws InterruptedException {
        return invokeOnFirst(delegate.nextIfAvailable(timeout, unit));
    }

    @Override
    public T next() throws InterruptedException {
        return invokeOnFirst(delegate.next());
    }

    @Override
    public void onAvailable(Runnable callback) {
        delegate.onAvailable(callback);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public Optional<Throwable> getError() {
        return delegate.getError();
    }

    private T invokeOnFirst(T element) {
        if (element != null && firstConsumed.compareAndSet(false, true)) {
            doOnFirstCallback.accept(element);
        }
        return element;
    }
}
