/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.event.transformation.event;

import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.function.Predicate;

/**
 * Stream of events that can be filtered and transformed.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public interface TransformableEventStream {

    /**
     * Filters events based on the provided {@code predicate}.
     *
     * @param predicate the predicate used for filtering events
     * @return filtered stream
     */
    TransformableEventStream filter(Predicate<EventWithToken> predicate);

    /**
     * Transforms events by providing transformation description and the transformer.
     *
     * @param transformationDescription the description of the transformation
     * @param eventTransformer          the transformer
     * @return the executor used to transform events
     */
    EventTransformationExecutor transform(String transformationDescription,
                                          EventTransformer eventTransformer);
}
