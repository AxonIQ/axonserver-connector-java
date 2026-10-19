/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.axonserver.connector.event;

/**
 * A connection to a persistent stream. Axon Server can assign zero or more segments to this connection.
 *
 *  @author Marc Gathier
 *  @since 2024.0.0
 */
public interface PersistentStream {

    /**
     * Closes the persistent stream.
     */
    void close();

}
