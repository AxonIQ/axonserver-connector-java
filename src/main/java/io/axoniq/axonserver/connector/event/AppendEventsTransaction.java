package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

public interface AppendEventsTransaction {

    AppendEventsTransaction appendEvent(Event event);

    CompletableFuture<Confirmation> commit();

    void rollback();
}
