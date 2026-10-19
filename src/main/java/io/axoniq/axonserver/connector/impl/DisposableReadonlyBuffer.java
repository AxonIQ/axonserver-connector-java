/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

/**
 * A {@link CloseableReadonlyBuffer buffer} that can be disposed of from the subscriber's side.
 *
 * @param <T> the type of messages in this buffer
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 * @since 4.6.0
 */
public interface DisposableReadonlyBuffer<T> extends CloseableReadonlyBuffer<T> {

    /**
     * Disposes this buffer from the subscriber's side.
     */
    void dispose();
}
