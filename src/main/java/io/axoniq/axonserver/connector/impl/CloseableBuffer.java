/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.grpc.ErrorMessage;

/**
 * A {@link CloseableReadonlyBuffer buffer} that can be closed by the producing side.
 *
 * @param <T> the type of messages in this buffer
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 * @since 4.6.0
 */
public interface CloseableBuffer<T> extends CloseableReadonlyBuffer<T> {

    /**
     * Puts a message in this buffer.
     *
     * @param message the message to be put in this buffer
     */
    void put(T message);

    /**
     * Closes this buffer from the producing side.
     */
    void close();

    /**
     * Closes this buffer exceptionally from the producing side.
     *
     * @param errorMessage an error indicating why this buffer is closed
     */
    void closeExceptionally(ErrorMessage errorMessage);
}
