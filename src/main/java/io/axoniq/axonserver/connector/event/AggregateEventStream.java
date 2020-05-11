package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Event;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface AggregateEventStream extends Spliterator<Event> {

    Event next() throws InterruptedException;

    boolean hasNext();

    void cancel();

    @Override
    default boolean tryAdvance(Consumer<? super Event> action) {
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
    default Spliterator<Event> trySplit() {
        return null;
    }

    @Override
    default long estimateSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    default int characteristics() {
        return Spliterator.NONNULL & Spliterator.ORDERED & Spliterator.IMMUTABLE & Spliterator.DISTINCT;
    }

    default Stream<Event> asStream() {
        return StreamSupport.stream(this, false);
    }

}
