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

package io.axoniq.axonserver.connector.event.transformation;

import java.util.concurrent.CompletableFuture;

/**
 * Communication channel for Event Transformation related interactions with AxonServer.
 *
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface EventTransformationChannel {

    /**
     * Returns the completable future of the all transformations.
     *
     * @return the completable future of the all transformations.
     */
    CompletableFuture<Iterable<EventTransformation>> transformations();

    /**
     * Returns the completable future of the active transformation.
     *
     * @return The completable future of the active transformation.
     */
    CompletableFuture<ActiveTransformation> activeTransformation();

    /**
     * Starts a new transformation to append events change actions.
     * It
     *
     * @param description the description of the transformation to start.
     * @return The completable future of the active transformation reference onto which to register events
     * transformation actions.
     */
    CompletableFuture<ActiveTransformation> newTransformation(String description);

    /**
     * Starts the compaction of the event store. The invocation of this method requests Axon Server to permanently
     * delete all the obsoleted version of the event store, in order to save storage space.
     *
     * @return a CompletableFuture that completes when the compaction request has been received by Axon Server.
     */
    CompletableFuture<Void> startCompacting();

    /**
     * This method conveniently composes the creation of a new transformation, the registration of all required changes
     * and the request to apply them to the event store, in a single invocation.
     *
     * @param description the description of the transformation
     * @param transformer the transformer used to register the required changes
     * @return a CompletableFuture of the EventTransformation
     */
    default CompletableFuture<EventTransformation> transform(String description, Transformer transformer) {
        return newTransformation(description)
                .thenCompose(transformation -> transformation.transform(transformer))
                .thenCompose(ActiveTransformation::startApplying);
    }
}
