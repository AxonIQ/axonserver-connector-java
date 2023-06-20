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
import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.event.DeletedEvent;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.TransformRequest;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
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

    private final Logger logger = LoggerFactory.getLogger(GrpcTransformationStream.class);
    private final String transformationId;
    private final Map<Long, CompletableFuture<Void>> pendingRequests = new ConcurrentHashMap<>();
    private final StreamObserver<TransformRequest> requestStreamObserver;
    private final AtomicReference<Consumer<Throwable>> onCompletedByServerListener = new AtomicReference<>();
    private final AtomicReference<Throwable> completed = new AtomicReference<>();

    public GrpcTransformationStream(String transformationId,
                                    StreamObserver<TransformRequest> requestStreamObserver,
                                    Consumer<Consumer<Long>> ackListenerRegistration,
                                    Consumer<Consumer<Throwable>> onCompleteRegistration) {
        this.transformationId = transformationId;
        this.requestStreamObserver = requestStreamObserver;
        ackListenerRegistration.accept(this::onTransformationActionAck);
        onCompleteRegistration.accept(this::completedByServer);
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
        checkCompleted();
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        pendingRequests.put(sequence, completableFuture);
        logger.trace("Sending delete event {} to Axon Server.", token);
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

    private void checkCompleted() {
        Throwable throwable = this.completed.get();
        if (throwable != null) {
            throw new StreamClosedException(throwable);
        }
    }

    @Override
    public CompletableFuture<Void> replaceEvent(long token, Event event, long sequence) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        pendingRequests.put(sequence, completableFuture);
        logger.trace("Sending replace event {} to Axon Server.", token);
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
        completed.compareAndSet(null, new RuntimeException("Completed by the client"));
        requestStreamObserver.onCompleted();
    }

    @Override
    public void onCompletedByServer(Consumer<Throwable> onCompleted) {
        onCompletedByServerListener.set(onCompleted);
    }

    private void completedByServer(Throwable error) {
        completed.compareAndSet(null, error);
        logger.warn("Transformation stream completed by server with error: ", error);
        completePending(error);
        Consumer<Throwable> completedByServerCallback = onCompletedByServerListener.get();
        if (completedByServerCallback != null) {
            completedByServerCallback.accept(error);
        }
    }

    private void completePending(Throwable error) {
        pendingRequests.forEach((sequence, result) -> {
            result.completeExceptionally(error);
            pendingRequests.remove(sequence);
        });
    }

    private void onTransformationActionAck(long sequence) {
        logger.trace("Acknowledge received for transformation sequence {}. ", sequence);
        pendingRequests.computeIfPresent(sequence,
                                         (key, appendOperation) -> {
                                             appendOperation.complete(null);
                                             return null; //this removes the key from the map
                                         });
    }
}
