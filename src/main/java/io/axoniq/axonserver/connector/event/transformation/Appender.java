/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.event.transformation;

import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * Appends the transformation actions.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public interface Appender {

    /**
     * Appends to the transformation the request to delete the event with the given token.
     *
     * @param token the token of the event to be deleted
     * @return a {@link CompletableFuture} of the Appender, used for composition
     */
    CompletableFuture<Appender> deleteEvent(long token);

    /**
     * Appends to the transformation the request to replace the event with the given token.
     *
     * @param token       the token of the event to be replaced
     * @param replacement the new event used to replace the original one
     * @return a {@link CompletableFuture} of the Appender, used for composition
     */
    CompletableFuture<Appender> replaceEvent(long token, Event replacement);
}
