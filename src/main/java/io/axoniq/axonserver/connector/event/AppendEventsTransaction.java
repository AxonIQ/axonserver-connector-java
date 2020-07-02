package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * Interface providing operations to interact with a Transaction to append events onto the Event Store.
 */
public interface AppendEventsTransaction {

    /**
     * Append the given event to be committed as part of this transaction.
     *
     * @param event The event to append
     *
     * @return this instance for fluent interfacing
     */
    AppendEventsTransaction appendEvent(Event event);

    /**
     * Commit this transaction, appending all registered events into the event store.
     *
     * @return a CompletableFuture resolving the confirmation of the successful processing of the transaction.
     */
    CompletableFuture<Confirmation> commit();

    /**
     * Rolls back the transaction.
     */
    void rollback();
}
