/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.ConfirmationWithConsistencyMarker;
import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * Interface providing operations to interact with a Transaction to append events onto the Event Store.
 */
public interface AppendEventsTransaction {

    /**
     * Append the given event to be committed as part of this transaction.
     *
     * @param event the event to append
     * @return this instance for fluent interfacing
     */
    AppendEventsTransaction appendEvent(Event event);

    /**
     * Commit this transaction, appending all registered events into the Event Store.
     *
     * @return a CompletableFuture resolving the confirmation of the successful processing of the transaction
     */
    CompletableFuture<ConfirmationWithConsistencyMarker> commit();

    /**
     * Rolls back the transaction.
     */
    void rollback();
}
