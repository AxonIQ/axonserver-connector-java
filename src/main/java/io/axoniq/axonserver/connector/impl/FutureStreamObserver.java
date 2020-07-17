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

package io.axoniq.axonserver.connector.impl;

import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;

/**
 * An implementation of both a {@link CompletableFuture} and {@link StreamObserver}. This future will complete
 * successfully on {@link #onNext(Object)} or on {@link #onCompleted()} (granted that the given {@code
 * valueWhenNoResult} is not of type {@link Throwable}). On {@link #onError(Throwable)}, this future will {@link
 * #completeExceptionally(Throwable)}.
 *
 * @param <T> the type of result this {@link CompletableFuture} and {@link StreamObserver} resolves
 */
public class FutureStreamObserver<T> extends CompletableFuture<T> implements StreamObserver<T> {

    private final Object valueWhenNoResult;

    /**
     * Construct a {@link FutureStreamObserver}, using the given {@code valueWhenNoResult} if no result is returned.
     *
     * @param valueWhenNoResult the object of type {@code T} to return if nothing is returned by this {@link
     *                          StreamObserver}
     */
    public FutureStreamObserver(T valueWhenNoResult) {
        this.valueWhenNoResult = valueWhenNoResult;
    }

    /**
     * Construct a {@link FutureStreamObserver}, using the given {@code valueWhenNoResult} if no result is returned.
     * This future will complete exceptionally when the {@code valueWhenNoResult} is used.
     *
     * @param valueWhenNoResult the {@link Throwable} to return if nothing is returned by this {@link StreamObserver}
     */
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
