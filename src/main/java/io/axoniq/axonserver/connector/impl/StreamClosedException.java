/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

/**
 * A {@link RuntimeException} to throw if a stream is closed.
 */
public class StreamClosedException extends RuntimeException {

    /**
     * Constructs a new {@link StreamClosedException} using the given {@code cause}.
     *
     * @param cause the {@link Throwable} further defining this {@link StreamClosedException}
     */
    public StreamClosedException(Throwable cause) {
        super(cause);
    }
}
