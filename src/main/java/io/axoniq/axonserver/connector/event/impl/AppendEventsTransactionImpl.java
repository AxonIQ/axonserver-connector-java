/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.event.AppendEventsTransaction;
import io.axoniq.axonserver.grpc.event.ConfirmationWithConsistencyMarker;
import io.axoniq.axonserver.grpc.event.Event;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;

/**
 * Implementation of the {@link AppendEventsTransaction} used to append events to the Event Store.
 */
public class AppendEventsTransactionImpl implements AppendEventsTransaction {

    private final StreamObserver<Event> stream;
    private final CompletableFuture<ConfirmationWithConsistencyMarker> result;

    /**
     * Constructs a {@link AppendEventsTransactionImpl} used to append events to the Event Store.
     *
     * @param stream the {@link StreamObserver} of {@link Event} instances which should be appended
     * @param result a {@link CompletableFuture} resolving the {@link ConfirmationWithConsistencyMarker} of the successful processing of this
     *               transaction
     */
    public AppendEventsTransactionImpl(StreamObserver<Event> stream, CompletableFuture<ConfirmationWithConsistencyMarker> result) {
        this.stream = stream;
        this.result = result;
    }

    @Override
    public AppendEventsTransaction appendEvent(Event event) {
        stream.onNext(event);
        return this;
    }

    @Override
    public CompletableFuture<ConfirmationWithConsistencyMarker> commit() {
        stream.onCompleted();
        return result;
    }

    @Override
    public void rollback() {
        stream.onError(new StatusRuntimeException(Status.CANCELLED));
    }
}
