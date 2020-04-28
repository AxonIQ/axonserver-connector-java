package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

public interface AppendEventsTransaction {

    void appendEvent(Event event);

    CompletableFuture<?> commit();

    void rollback();
}
