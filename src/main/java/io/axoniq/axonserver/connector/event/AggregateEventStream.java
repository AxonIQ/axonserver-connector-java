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

package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.event.Event;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A stream of Events for a single Aggregate. The operations on this stream are blocking and intended for Event Sourcing
 * purposes, where an Aggregate's state needs to be reconstructed based on historic events.
 */
public interface AggregateEventStream {

    /**
     * Returns the next available event, possibly blocking until one becomes available for reading.
     *
     * @return the next event
     * @throws InterruptedException  if the current thread was interrupted while waiting for an event to become
     *                               available
     * @throws StreamClosedException when the stream has reached the end or was closed for another reason
     * @see #hasNext() to verify availability of events
     */
    Event next() throws InterruptedException;

    /**
     * Indicates whether a new event is available. This method may block while waiting for a confirmation if an event is
     * available for reading.
     *
     * @return {@code true} if a message is available, or {@code false} if the stream has reached the end
     * @throws StreamClosedException is the stream has been closed prematurely because of an error or on client request
     */
    boolean hasNext();

    /**
     * Close this stream for further reading, notifying the provider of Events to stop streaming them. Any event already
     * emitted by the sender may still be consumed.
     * <p>
     * Note that after calling {@code cancel}, {@link #hasNext()} may throw a StreamClosedException if the cancellation
     * caused the stream to close before the last message was received.
     */
    void cancel();

    /**
     * Returns a Stream that consumes the Events from this instance. Note that this instance should not be read from in
     * parallel to the returned stream, as this may provide undefined results on the availability of events in either
     * stream instance.
     *
     * @return a Stream containing the Events contained in this stream
     */
    default Stream<Event> asStream() {
        return StreamSupport.stream(new Spliterator<Event>() {
            @Override
            public boolean tryAdvance(Consumer<? super Event> action) {
                if (hasNext()) {
                    try {
                        action.accept(next());
                        return true;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
                return false;
            }

            @Override
            public Spliterator<Event> trySplit() {
                return null;
            }

            @Override
            public long estimateSize() {
                return Integer.MAX_VALUE;
            }

            @Override
            public int characteristics() {
                return Spliterator.NONNULL & Spliterator.ORDERED & Spliterator.IMMUTABLE & Spliterator.DISTINCT;
            }
        }, false);
    }
}
