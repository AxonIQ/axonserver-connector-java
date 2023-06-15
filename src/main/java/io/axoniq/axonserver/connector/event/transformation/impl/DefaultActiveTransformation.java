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

import io.axoniq.axonserver.connector.event.transformation.ActiveTransformation;
import io.axoniq.axonserver.connector.event.transformation.EventTransformation;
import io.axoniq.axonserver.connector.event.transformation.Transformer;
import io.axoniq.axonserver.connector.event.transformation.impl.EventTransformationService.TransformationStream;

import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of the {@link ActiveTransformation}
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public class DefaultActiveTransformation implements ActiveTransformation {

    private final String transformationId;
    private final Long currentSequence;
    private final EventTransformationService service;

    /**
     * Constructs an instance based on the specified parameters.
     *
     * @param transformationId the identifier of the active transformation
     * @param currentSequence  the current last sequence, used as validation that no action has been lost
     * @param service          the {@link EventTransformationService} used to communicate with Axon Server
     */
    DefaultActiveTransformation(String transformationId,
                                Long currentSequence,
                                EventTransformationService service) {
        this.transformationId = transformationId;
        this.currentSequence = currentSequence;
        this.service = service;
    }

    @Override
    public CompletableFuture<ActiveTransformation> transform(Transformer transformer) {
        TransformationStream transformationStream = service.transformationStream(transformationId); //open stream
        ActionAppender appender = new ActionAppender(transformationStream, currentSequence);
        transformer.transform(appender); //execute transformation
        return appender.complete()           //close stream
                       .thenApply(seq -> new DefaultActiveTransformation(transformationId,
                                                                         seq,
                                                                         service));
    }

    @Override
    public CompletableFuture<EventTransformation> startApplying() {
        return service.startApplying(transformationId, currentSequence)
                      .thenCompose(unused -> service.transformationById(transformationId));
    }

    @Override
    public CompletableFuture<EventTransformation> cancel() {
        return service.cancel(transformationId)
                      .thenCompose(unused -> service.transformationById(transformationId));
    }
}
