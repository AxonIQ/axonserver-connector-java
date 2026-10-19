/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

/**
 * A {@link RuntimeException} to throw if a stream is completed unexpectedly.
 *
 * @author Sara Pellegrini
 * @since 4.4.3
 */
public class StreamUnexpectedlyCompletedException extends RuntimeException {

    /**
     * Constructs a new instance of {@link StreamUnexpectedlyCompletedException}.
     */
    public StreamUnexpectedlyCompletedException() {
    }

    /**
     * Constructs a new instance of {@link StreamUnexpectedlyCompletedException} with a detailed description of the
     * cause of the exception
     *
     * @param message the message describing the cause of the exception.
     */
    public StreamUnexpectedlyCompletedException(String message) {
        super(message);
    }
}
