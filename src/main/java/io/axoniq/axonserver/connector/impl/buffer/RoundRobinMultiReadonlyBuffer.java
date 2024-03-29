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

import io.axoniq.axonserver.connector.impl.AssertUtils;
import io.axoniq.axonserver.connector.impl.CloseableReadonlyBuffer;
import io.axoniq.axonserver.connector.impl.DisposableReadonlyBuffer;
import io.axoniq.axonserver.grpc.ErrorMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of the {@link DisposableReadonlyBuffer} that operates across multiple {@link
 * DisposableReadonlyBuffer} instances. Operations are delegated to these instances in a round-robin fashion.
 *
 * @param <T> the type of messages this buffer contain
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 * @since 4.6.0
 */
public class RoundRobinMultiReadonlyBuffer<T> implements DisposableReadonlyBuffer<T> {

    private final List<DisposableReadonlyBuffer<T>> buffers;
    private final AtomicInteger indexer = new AtomicInteger();

    /**
     * Instantiates this buffer with a list of delegates.
     *
     * @param buffers a list of delegates
     */
    public RoundRobinMultiReadonlyBuffer(List<? extends DisposableReadonlyBuffer<T>> buffers) {
        AssertUtils.assertParameter(buffers != null, "buffers must not be null");
        AssertUtils.assertParameter(!buffers.isEmpty(), "buffers must not be empty");
        this.buffers = new ArrayList<>(buffers);
    }

    /**
     * Polls the element from one of the buffers in a round-robin fashion.
     *
     * @return an {@link Optional} with polled message
     * @see DisposableReadonlyBuffer#poll()
     */
    @Override
    public Optional<T> poll() {
        for (int i = 0; i < buffers.size(); i++) {
            Optional<T> read = buffers.get(nextPosition())
                                      .poll();
            if (read.isPresent()) {
                return read;
            }
        }
        return Optional.empty();
    }

    /**
     * Checks whether all buffers are closed.
     *
     * @return {@code true} if all buffers are closed, {@code false} otherwise
     * @see DisposableReadonlyBuffer#closed()
     */
    @Override
    public boolean closed() {
        return buffers.stream()
                      .map(CloseableReadonlyBuffer::closed)
                      .reduce(true, Boolean::logicalAnd);
    }

    /**
     * Retrieves an error from the first buffer if all buffers errored out.
     *
     * @return the error from the first buffer if all buffers errored out
     * @see DisposableReadonlyBuffer#error()
     */
    @Override
    public Optional<ErrorMessage> error() {
        boolean allInError = buffers.stream()
                                    .map(b -> b.error().isPresent())
                                    .reduce(Boolean::logicalAnd)
                                    .orElse(false);
        return allInError ? buffers.get(0).error() : Optional.empty();
    }

    /**
     * Checks whether all buffers are empty.
     *
     * @return {@code true} if all buffers are empty, {@code false} otherwise
     * @see DisposableReadonlyBuffer#isEmpty()
     */
    @Override
    public boolean isEmpty() {
        return buffers.stream()
                      .map(CloseableReadonlyBuffer::isEmpty)
                      .reduce(true, Boolean::logicalAnd);
    }

    /**
     * Sums up capacities of all buffers.
     *
     * @return the sum of capacities of all buffers
     * @see DisposableReadonlyBuffer#capacity()
     */
    @Override
    public int capacity() {
        return buffers.stream()
                      .map(CloseableReadonlyBuffer::capacity)
                      .reduce(0, Integer::sum);
    }

    /**
     * Registers {@link Runnable onAvailable} handler to all buffers
     *
     * @param onAvailable to be invoked when there are changes in buffers
     * @see DisposableReadonlyBuffer#onAvailable(Runnable)
     */
    @Override
    public void onAvailable(Runnable onAvailable) {
        buffers.forEach(b -> b.onAvailable(onAvailable));
    }

    /**
     * Calls a {@link DisposableReadonlyBuffer#dispose()} on each and every buffer.
     *
     * @see DisposableReadonlyBuffer#dispose()
     */
    @Override
    public void dispose() {
        buffers.forEach(DisposableReadonlyBuffer::dispose);
    }

    private int nextPosition() {
        return indexer.getAndUpdate(this::nextPositionBounded);
    }

    private int nextPositionBounded(int current) {
        return current + 1 == buffers.size() ? 0 : current + 1;
    }
}
