package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Event;

public interface AggregateEventStream {

    Event next() throws InterruptedException;

    boolean hasNext();

    void close();
}
