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

package io.axoniq.axonserver.connector;

import java.util.concurrent.TimeUnit;

/**
 * Interface describing a stream of results to a request. This stream allows for blocking retrieval of elements, as well
 * as a non-blocking approach using notifications when messages are available.
 * <p>
 * While the ResultStream itself is safe for multi-threaded use, some methods may only provide best-effort estimates. In
 * such case, for example, there is no guarantee that a {@code peek()} followed by a {@code nextIfAvailable()} will
 * yield the same message.
 *
 * @param <T> the result type returned by this {@link ResultStream}
 */
public interface ResultStream<T> extends AutoCloseable {

    /**
     * Returns the next available element in the stream, if available, without consuming it from the stream. Will return
     * {@code null} when no element is available, or when the stream has been closed.
     *
     * @return the next available element in the stream, or {@code null} if none is available, or if the stream is
     * closed
     */
    T peek();

    /**
     * Consumes the next available element in the stream, if available. Will return {@code null} if no element is
     * available immediately.
     *
     * @return the next available element in the stream, or {@code null} if none is available, or if the stream is
     * closed
     */
    T nextIfAvailable();

    /**
     * Consumes the next available element in the stream, waiting for at most {@code timeout} (in given {@code unit})
     * for an element to become available. If no element is available with the given timeout, or if the stream has been
     * closed, it returns {@code null}.
     *
     * @param timeout the amount of time to wait for an element to become available
     * @param unit    the unit of time in which the timeout is expressed
     * @return the next available element in the stream, or {@code null} if none is available
     * @throws InterruptedException when the Thread is interrupted while waiting for an element to become available
     * @see #isClosed()
     */
    T nextIfAvailable(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Consumes the next available element in the stream, waiting for an element to become available, or for the buffer
     * to be closed.
     *
     * @return the next available element in the stream, or {@code null} if none available
     * @throws InterruptedException                                      when the Thread is interrupted while waiting
     *                                                                   for an element to become available
     * @throws io.axoniq.axonserver.connector.impl.StreamClosedException when the stream is closed
     * @see #isClosed()
     */
    T next() throws InterruptedException;

    // TODO - Allow for a method the returns a CompletableFuture<T> which completes when the next available item is available (or immediately, if one is already available).

    /**
     * Sets the given {@code callback} to execute when data is available for reading, or the stream has been closed.
     * Note that <em>any</em> registration will replace the previous one.
     * <p>
     * The callback is invoked in the publisher's thread. Invocations should be as short as possible, preferably
     * delegating to a reader thead, instead of accessing the entries directly.
     *
     * @param callback a {{@link Runnable}} to {@link Runnable#run()} when the next entry becomes available
     */
    void onAvailable(Runnable callback);

    /**
     * Requests the current stream to be closed. It is up to the publishing end of the stream to honor the request.
     * <p>
     * Any elements received before closing are still available for reading.
     */
    void close();

    /**
     * Indicates whether the current stream is closed for further reading. Note that this method will also return {@code
     * false} in case the stream is closed by the element provider, but there are still elements awaiting consumption.
     *
     * @return {@code true} if the stream is closed for further reading, otherwise {@code false}
     */
    boolean isClosed();
}
