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
 * Class responsible to interact with the active transformation.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public interface ActiveTransformation {

    /**
     * Accepts a {@link Transformer} to collect the requested changes and to register them into the active
     * transformation.
     *
     * @param transformer the {@link Transformer} used to append the required changes to the active transformation
     * @return a {@link CompletableFuture} of the active transformation that completes when all changes have been
     * registered.
     */
    CompletableFuture<ActiveTransformation> transform(Transformer transformer);

    /**
     * Requests the transformation to be applied. After this method is invoked, it will not be possible to cancel the
     * transformation or to register new changes in it. Axon Server will asynchronously apply the transformation to
     * the event store, making the changes effective.
     * The returned {@link CompletableFuture} completes when the request to start applying has been received form AS,
     * it does not wait the apply process is completed. Depending on the number of transformation actions contained in
     * the transformation, the apply process can take a long time before to be completed.
     *
     * @return a {@link CompletableFuture} of the Event Transformation, that completes only after the start applying
     * request has been received by AS.
     */
    CompletableFuture<EventTransformation> startApplying();

    /**
     * Cancels the active transformation. After this method is invoked, it will not be possible to apply the
     * transformation or to register new changes in it.
     *
     * @return a {@link CompletableFuture} of the Event Transformation, that completes only after the cancel request has
     * been received by AS.
     */
    CompletableFuture<EventTransformation> cancel();
}
