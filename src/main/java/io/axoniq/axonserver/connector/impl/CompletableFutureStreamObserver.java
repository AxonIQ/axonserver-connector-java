package io.axoniq.axonserver.connector.impl;

import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * {@link StreamObserver} that completes a {@link CompletableFuture} at the first response or when
 *
 * @author Sara Pellegrini
 * @since 4.6
 */
public class CompletableFutureStreamObserver<IN, RESPONSE> implements StreamObserver<IN> {

    private final CompletableFuture<RESPONSE> response;
    private final Function<IN, RESPONSE> responseMapper;
    private final Function<Throwable, Throwable> errorMapper;

    public CompletableFutureStreamObserver(CompletableFuture<RESPONSE> response) {
        this(response, next -> (RESPONSE) next);
    }

    public CompletableFutureStreamObserver(CompletableFuture<RESPONSE> response,
                                           Function<IN, RESPONSE> responseMapper) {
        this(response, responseMapper, UnaryOperator.identity());
    }

    public CompletableFutureStreamObserver(CompletableFuture<RESPONSE> response,
                                           Function<IN, RESPONSE> responseMapper,
                                           UnaryOperator<Throwable> errorMapper) {
        this.response = response;
        this.responseMapper = responseMapper;
        this.errorMapper = errorMapper;
    }

    @Override
    public void onNext(IN value) {
        if (!response.isDone()) {
            try {
                response.complete(responseMapper.apply(value));
            } catch (Exception e) {
                response.completeExceptionally(e);
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        if (!response.isDone()) {
            response.completeExceptionally(errorMapper.apply(throwable));
        }
    }

    @Override
    public void onCompleted() {
        if (!response.isDone()) {
            response.complete(null);
        }
    }
}
