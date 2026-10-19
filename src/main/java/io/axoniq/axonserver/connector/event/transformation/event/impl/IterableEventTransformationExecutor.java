/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.event.transformation.event.impl;

import io.axoniq.axonserver.connector.event.transformation.Appender;
import io.axoniq.axonserver.connector.event.transformation.EventTransformation;
import io.axoniq.axonserver.connector.event.transformation.EventTransformationChannel;
import io.axoniq.axonserver.connector.event.transformation.event.EventTransformationExecutor;
import io.axoniq.axonserver.connector.event.transformation.event.EventTransformer;
import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * {@link EventTransformationExecutor} implementation based on an {@link Iterable} of {@link EventWithToken}.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public class IterableEventTransformationExecutor implements EventTransformationExecutor {

    private final String transformationDescription;
    private final Iterable<EventWithToken> events;
    private final EventTransformer eventTransformer;

    /**
     * Constructs an instance based on the specified parameters.
     *
     * @param transformationDescription the description of the transformation
     * @param events                    the events to be transformed
     * @param eventTransformer          the transformation function
     */
    IterableEventTransformationExecutor(String transformationDescription,
                                        Iterable<EventWithToken> events,
                                        EventTransformer eventTransformer) {
        this.transformationDescription = transformationDescription;
        this.events = events;
        this.eventTransformer = eventTransformer;
    }

    @Override
    public CompletableFuture<EventTransformation> execute(Supplier<EventTransformationChannel> channelSupplier) {
        return channelSupplier.get()
                              .transform(transformationDescription, this::transform);
    }

    private void transform(Appender appender) {
        events.forEach(event -> eventTransformer.accept(event, appender));
    }
}
