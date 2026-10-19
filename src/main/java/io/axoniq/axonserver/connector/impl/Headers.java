/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.grpc.Metadata;

/**
 * Utility class containing header definitions.
 */
public abstract class Headers {

    /**
     * A {@link Metadata.Key} defining the context from within which a message will be send.
     */
    public static final Metadata.Key<String> CONTEXT =
            Metadata.Key.of("AxonIQ-Context", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * A {@link Metadata.Key} defining the access token from the application sending this message.
     */
    public static final Metadata.Key<String> ACCESS_TOKEN =
            Metadata.Key.of("AxonIQ-Access-Token", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * A {@link Metadata.Key} defining the client ID of the application sending this message.
     */
    public static final Metadata.Key<String> CLIENT_ID =
            Metadata.Key.of("AxonIQ-ClientId", Metadata.ASCII_STRING_MARSHALLER);

    private Headers() {
        // Utility class
    }
}
