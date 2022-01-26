/*
 * Copyright (c) 2020-2022. AxonIQ
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

package io.axoniq.axonserver.connector.impl.buffer;

import io.axoniq.axonserver.connector.impl.CloseableBuffer;
import io.axoniq.axonserver.grpc.ErrorMessage;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An implementation of {@link CloseableBuffer} that uses {@link LinkedBlockingQueue} as backing buffer.
 *
 * @param <T> the type of messages in this buffer
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 */
public class BlockingCloseableBuffer<T> implements CloseableBuffer<T> {

    private final int capacity = 32;

    private final BlockingQueue<T> buffer = new LinkedBlockingQueue<>(capacity);
    private volatile boolean closed = false;
    private volatile ErrorMessage error;
    private final AtomicReference<Runnable> onAvailable = new AtomicReference<>();

    @Override
    public Optional<T> poll() {
        return Optional.ofNullable(buffer.poll());
    }

    @Override
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    @Override
    public int capacity() {
        return capacity;
    }

    /**
     * Returns the number of elements in this buffer.
     *
     * @return the number of elements in this buffer
     */
    public int size() {
        return buffer.size();
    }

    @Override
    public void onAvailable(Runnable onAvailable) {
        this.onAvailable.set(onAvailable);
        if (!isEmpty() || closed) {
            notifyOnAvailable();
        }
    }

    @Override
    public void put(T message) {
        try {
            buffer.put(message);
            notifyOnAvailable();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean closed() {
        return closed;
    }

    @Override
    public Optional<ErrorMessage> error() {
        return Optional.ofNullable(error);
    }

    @Override
    public void close() {
        closed = true;
        notifyOnAvailable();
    }

    @Override
    public void closeExceptionally(ErrorMessage errorMessage) {
        error = errorMessage;
        close();
    }

    protected void notifyOnAvailable() {
        Runnable onAvailable = this.onAvailable.get();
        if (onAvailable != null) {
            onAvailable.run();
        }
    }
}