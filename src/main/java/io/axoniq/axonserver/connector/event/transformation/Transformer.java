/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.event.transformation;

/**
 * Functional interface that needs to be implemented to register transformation actions into the Transformation.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
@FunctionalInterface
public interface Transformer {

    /**
     * Registers the requested event changes to the event store.
     *
     * @param appender the appender used to register the transformation actions into the Transformation, since it
     *                 provides the methods to delete and replace events
     */
    void transform(Appender appender);
}
