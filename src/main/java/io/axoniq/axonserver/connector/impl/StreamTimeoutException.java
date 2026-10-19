/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

/**
 * A {@link RuntimeException} to throw if reading from a stream results in a timeout.
 *
 * @author Mitchell Herrijgers
 * @since 4.5.6
 */
public class StreamTimeoutException extends RuntimeException {

    /**
     * Constructs a new instance of {@link StreamTimeoutException}.
     */
    public StreamTimeoutException() {
    }

    /**
     * Constructs a new instance of {@link StreamTimeoutException} with a detailed description of the cause of the
     * exception
     *
     * @param message the message describing the cause of the exception.
     */
    public StreamTimeoutException(String message) {
        super(message);
    }
}
