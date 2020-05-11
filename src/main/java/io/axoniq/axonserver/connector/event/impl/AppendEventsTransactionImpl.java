package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.event.AppendEventsTransaction;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;

public class AppendEventsTransactionImpl implements AppendEventsTransaction {

    private final StreamObserver<Event> stream;
    private final CompletableFuture<Confirmation> result;

    public AppendEventsTransactionImpl(StreamObserver<Event> stream, CompletableFuture<Confirmation> result) {
        this.stream = stream;
        this.result = result;
    }

    @Override
    public AppendEventsTransaction appendEvent(Event event) {
        stream.onNext(event);
        return this;
    }

    @Override
    public CompletableFuture<Confirmation> commit() {
        stream.onCompleted();
        return result;
    }

    @Override
    public void rollback() {
        stream.onError(new StatusRuntimeException(Status.CANCELLED));
    }
}
