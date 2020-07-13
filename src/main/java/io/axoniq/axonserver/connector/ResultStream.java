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
 * @param <T>
 */
public interface ResultStream<T> extends AutoCloseable {
    /**
     * Returns the next available element in the stream, if available, without consuming it from the stream. Will
     * return {@code null} when no element is available, or when the stream has been closed.
     *
     * @return the next available element in the stream, or {@code null} if none available, or if the stream is closed
     */
    T peek();

    /**
     * Consumes the next available element in the stream, if available. Will return {@code null} if no element is
     * available immediately.
     *
     * @return the next available element in the stream, or {@code null} if none available or if the stream is closed
     */
    T nextIfAvailable();

    /**
     * Consumes the next available element in the stream, waiting for at most {@code timeout} (in given {@code unit}
     * for an element to become available. Of no element is available with the given timeout, or if the stream has been
     * closed, it returns {@code null}.
     *
     * @param timeout The amount of time to wait for an element to become available
     * @param unit    The unit of time in which the timeout is expressed
     *
     * @return the next available element in the stream, or {@code null} if none available
     * @throws InterruptedException when the Thread is interrupted while waiting
     *                              for an element to become available
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
     * Sets the callback to execute when data is available for reading, or the stream has been closed. Note that any
     * registration will replace the previous one.
     * <p>
     * The callback is invoked in the publisher's thread. Invocations should be as short as possible, preferably
     * delegating to a reader thead, instead of accessing the entries directly.
     *
     * @param r
     */
    void onAvailable(Runnable r);

    /**
     * Requests the current stream to be closed. It is up to the publisher end of the stream to honor the request.
     * <p>
     * Any elements received before closing are still available for reading.
     */
    void close();

    /**
     * Indicates whether the current stream is closed for further reading. Note that this method will also return
     * {@code false} in case the stream is closed by the element provider, but there are still elements awaiting
     * consumption.
     *
     * @return {@code true} if the stream is closed for further reading, otherwise {@code false}
     */
    boolean isClosed();
}
