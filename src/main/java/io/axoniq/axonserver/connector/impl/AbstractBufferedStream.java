/*
 * Copyright (c) 2020. AxonIQ
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

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ResultStream;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstract {@link FlowControlledBuffer} and {@link ResultStream} implementation, used to provide a buffered stream
 * of results of {@code T} when receiving a request of type {@code R}.
 *
 * @param <T> the type of results this stream implementation buffers
 * @param <R> the type of message used for flow control on this stream
 */
public abstract class AbstractBufferedStream<T, R> extends FlowControlledBuffer<T, R> implements ResultStream<T> {

    private static final Runnable NO_OP = () -> {
    };

    private final AtomicReference<Runnable> onAvailableCallback = new AtomicReference<>(NO_OP);

    /**
     * Constructs an {@link AbstractBufferedStream}
     *
     * @param clientId    the identifier of the client initiation this stream
     * @param bufferSize  the size of this buffer
     * @param refillBatch the number of entries to be consumed prior to refilling this buffer
     */
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
