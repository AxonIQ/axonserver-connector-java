/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.event.transformation.impl;

/**
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public class TransformationStreamCompletedException extends RuntimeException {

    public TransformationStreamCompletedException() {
    }

    public TransformationStreamCompletedException(String message) {
        super(message);
    }
}
