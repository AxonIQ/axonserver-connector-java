/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.event.transformation.event;

import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.transformation.event.impl.TransformableEventIterable;
import io.axoniq.axonserver.connector.event.transformation.event.stream.TokenRangeEvents;
import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.function.Supplier;

/**
 * Provides various ways to get {@link TransformableEventStream}.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public interface EventSources {

    /**
     * Creates a {@link TransformableEventStream} based on the {@link Iterable} of events.
     *
     * @param events {@link Iterable} of events
     * @return {@link TransformableEventStream}
     */
    static TransformableEventStream fromIterable(Iterable<EventWithToken> events) {
        return new TransformableEventIterable(events);
    }

    /**
     * Creates a {@link TransformableEventStream} based on the {@code eventChannelSupplier} and {@code first} and
     * {@code last} event tokens.
     *
     * @param eventChannelSupplier supplies the {@link EventChannel}
     * @param first                the first token to be included in the stream, should be -1 to start from the
     *                             beginning
     * @param last                 the last token to be included in the stream
     * @return {@link TransformableEventStream}
     */
    static TransformableEventStream range(Supplier<EventChannel> eventChannelSupplier, long first, long last) {
        return EventSources.fromIterable(new TokenRangeEvents(eventChannelSupplier, first, last));
    }
}
