/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.grpc.ErrorMessage;

import java.util.Optional;

/**
 * A readonly buffer that can be closed from the producing side.
 *
 * @param <T> the type of messages in this buffer
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 * @since 4.6.0
 */
public interface CloseableReadonlyBuffer<T> {

    /**
     * Polls the message from this buffer. If the returned {@link Optional} is empty, that doesn't mean that this buffer
     * stays empty forever. The {@link #closed()} method should be consolidated to validate this.
     *
     * @return an {@link Optional} with polled message
     */
    Optional<T> poll();

    /**
     * Indicates whether there are messages in the buffer. If this returns {@code false}, that doesn't mean that this
     * buffer stays empty forever. The {@link #closed()} method should be consolidated to validate this.
     *
     * @return {@code true} if buffer is empty, {@code false} otherwise
     */
    boolean isEmpty();

    /**
     * Returns the overall capacity of this buffer.
     *
     * @return the overall capacity of this buffer
     */
    int capacity();

    /**
     * Registers an action to be triggered when there is a new message added to the buffer, or the buffer is closed, or
     * the buffer is closed with an error. The action can check {@link #poll()}, {@link #closed()}, and {@link #error()}
     * to validate this.
     *
     * @param onAvailable to be invoked when there are changes in this buffer
     */
    void onAvailable(Runnable onAvailable);

    /**
     * Indicates whether the buffer is closed by the producing side.
     *
     * @return {@code true} if closed, {@code false} otherwise
     */
    boolean closed();

    /**
     * Returns an error from this buffer, if any.
     *
     * @return an {@link Optional} of an error, if any
     */
    Optional<ErrorMessage> error();
}
