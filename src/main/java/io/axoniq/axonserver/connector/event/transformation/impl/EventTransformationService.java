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

package io.axoniq.axonserver.connector.event.transformation.impl;

import io.axoniq.axonserver.connector.event.transformation.EventTransformation;
import io.axoniq.axonserver.grpc.event.Event;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Service to interact with AxonServer in respect to the event transformations.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public interface EventTransformationService {

    /**
     * Returns all the existing Event Transformations.
     *
     * @return a {@link CompletableFuture} of the Event Transformations
     */
    CompletableFuture<Iterable<EventTransformation>> transformations();

    /**
     * Returns the Event Transformations with the specified id.
     *
     * @param id the id of the {@link EventTransformation} to be returned.
     * @return a {@link CompletableFuture} of the {@link EventTransformation} with the specified id.
     */
    CompletableFuture<EventTransformation> transformationById(String id);

    /**
     * Creates a new transformation.
     *
     * @param description the description of the transformation to create.
     * @return a {@link CompletableFuture} of the identifier of the newly created transformation.
     */
    CompletableFuture<String> newTransformation(String description);

    /**
     * Opens a {@link TransformationStream} towards the transformation, that can be used to register the required
     * changes.
     *
     * @param transformationId the identifier of the transformation
     * @return the {@link TransformationStream}
     */
    TransformationStream transformationStream(String transformationId);

    /**
     * Requests to AxonServer to apply the transformation with the specified identifier.
     *
     * @param transformationId the identifier of the transformation to be applied
     * @param sequence         the expected last sequence of the transformation, used to check that no change request
     *                         has been lost
     * @return a {@link CompletableFuture} that completes when the request has been received by Axon Server.
     */
    CompletableFuture<Void> startApplying(String transformationId, Long sequence);

    /**
     * Requests to AxonServer to cancel the transformation with the specified identifier.
     *
     * @param transformationId the identifier of the transformation to be cancelled
     * @return a {@link CompletableFuture} that completes when the request has been received by Axon Server.
     */
    CompletableFuture<Void> cancel(String transformationId);

    /**
     * Requests to AxonServer to compact the event store. In other words, the request to delete all obsoleted version
     * of the events from the event store.
     *
     * @return a {@link CompletableFuture} that completes when the request has been received by Axon Server.
     */
    CompletableFuture<Void> startCompacting();

    /**
     * The stream toward the transformation needed to register the required events' changes.
     */
    interface TransformationStream {

        /**
         * Registers the request to delete the event with the specified token.
         *
         * @param token    the token of the event to be deleted
         * @param sequence the expected last sequence of the transformation, used to check that no change request
         *                 before this one has been lost
         * @return a {@link CompletableFuture} that completes when the request has been received by Axon Server.
         */
        CompletableFuture<Void> deleteEvent(long token, long sequence);

        /**
         * Registers the request to replace the event with the specified token.
         *
         * @param token    the token of the event to be replaced
         * @param event    the new version of the event, to be used as a replacement for the original one
         * @param sequence the expected last sequence of the transformation, used to check that no change request
         *                 before this one has been lost
         * @return a {@link CompletableFuture} that completes when the request has been received by Axon Server.
         */
        CompletableFuture<Void> replaceEvent(long token, Event event, long sequence);

        /**
         * Completes the stream.
         */
        void complete();

        /**
         * Resister a callback function that is invoked when the stream is completed by Axon Server.
         *
         * @param onCompleted the callback function invoked when the stream is completed by Axon Server.
         */
        void onCompletedByServer(Consumer<Optional<Throwable>> onCompleted);
    }
}
