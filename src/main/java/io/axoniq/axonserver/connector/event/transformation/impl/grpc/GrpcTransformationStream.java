/*
 * Copyright (c) 2020-2023. AxonIQ
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

package io.axoniq.axonserver.connector.event.transformation.impl.grpc;

import io.axoniq.axonserver.connector.event.transformation.impl.EventTransformationService.TransformationStream;
import io.axoniq.axonserver.grpc.event.DeletedEvent;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.TransformRequest;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class GrpcTransformationStream implements TransformationStream {

    private final Map<Long, CompletableFuture<Void>> pendingRequests = new ConcurrentHashMap<>();

    private final String transformationId;
    private final StreamObserver<TransformRequest> requestStreamObserver;
    private final AtomicReference<Consumer<Optional<Throwable>>> onCompletedRef = new AtomicReference<>(error -> {});

    public GrpcTransformationStream(String transformationId,
                                    StreamObserver<TransformRequest> requestStreamObserver,
                                    Consumer<Consumer<Long>> ackListenerRegistration,
                                    Consumer<Consumer<Optional<Throwable>>> onCompleteRegistration) {
        this.transformationId = transformationId;
        this.requestStreamObserver = requestStreamObserver;
        ackListenerRegistration.accept(this::onTransformationActionAck);
        onCompleteRegistration.accept(error -> onCompletedRef.get().accept(error));
    }

    @Nonnull
    private static TransformedEvent transformedEvent(long token, Event event) {
        return TransformedEvent.newBuilder()
                               .setEvent(event)
                               .setToken(token)
                               .build();
    }

    @Override
    public CompletableFuture<Void> deleteEvent(long token, long sequence) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        pendingRequests.put(sequence, completableFuture);
        requestStreamObserver.onNext(TransformRequest.newBuilder()
                                                     .setTransformationId(transformationId())
                                                     .setSequence(sequence)
                                                     .setDeleteEvent(deleteEvent(token))
                                                     .build());
        return completableFuture;
    }

    private DeletedEvent deleteEvent(long token) {
        return DeletedEvent.newBuilder()
                           .setToken(token)
                           .build();
    }

    @Override
    public CompletableFuture<Void> replaceEvent(long token, Event event, long sequence) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        pendingRequests.put(sequence, completableFuture);
        requestStreamObserver.onNext(TransformRequest.newBuilder()
                                                     .setTransformationId(transformationId())
                                                     .setSequence(sequence)
                                                     .setReplaceEvent(transformedEvent(token, event))
                                                     .build());
        return completableFuture;
    }

    @Nonnull
    private TransformationId transformationId() {
        return TransformationId.newBuilder()
                               .setId(transformationId)
                               .build();
    }

    @Override
    public void complete() {
        requestStreamObserver.onCompleted();
    }

    @Override
    public void onCompletedByServer(Consumer<Optional<Throwable>> onCompleted) {
        onCompletedRef.set(onCompleted);
    }

    private void onTransformationActionAck(long sequence) {
        pendingRequests.computeIfPresent(sequence,
                                         (key, appendOperation) -> {
                                             appendOperation.complete(null);
                                             return null; //this removes the key from the map
                                         });
    }
}
