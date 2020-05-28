package io.axoniq.axonserver.connector.impl;

import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;

public class FutureStreamObserver<T> extends CompletableFuture<T> implements StreamObserver<T> {

    private final Object valueWhenNoResult;

    public FutureStreamObserver(T valueWhenNoResult) {
        this.valueWhenNoResult = valueWhenNoResult;
    }

    public FutureStreamObserver(Throwable valueWhenNoResult) {
        this.valueWhenNoResult = valueWhenNoResult;
    }

    @Override
    public void onNext(T value) {
        complete(value);
    }

    @Override
    public void onError(Throwable t) {
        if (!isDone()) {
            completeExceptionally(t);
        }
    }

    @Override
    public void onCompleted() {
        if (!isDone()) {
            if (valueWhenNoResult instanceof Throwable) {
                completeExceptionally((Throwable) valueWhenNoResult);
            } else {
                //noinspection unchecked
                complete((T) valueWhenNoResult);
            }
        }
    }
}
