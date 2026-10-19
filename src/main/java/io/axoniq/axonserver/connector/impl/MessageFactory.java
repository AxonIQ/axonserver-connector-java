/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.grpc.ErrorMessage;

/**
 * Utility class to build simple messages.
 */
public abstract class MessageFactory {

    private MessageFactory() {
        // Utility class
    }

    /**
     * Build an {@link ErrorMessage}, using the given {@code errorCategory}, {@code client} and {@link Throwable}.
     *
     * @param errorCategory the {@link ErrorCategory} the constructed {@link ErrorMessage} belongs to
     * @param client        a {@link String} defining where the constructed {@link ErrorMessage} originates from
     * @param t             a {@link Throwable} defining the original exception resulting in this {@link ErrorMessage}
     * @return an {@link ErrorMessage}, using the given {@code errorCategory}, {@code client} and {@link Throwable}
     */
    public static ErrorMessage buildErrorMessage(ErrorCategory errorCategory, String client, Throwable t) {
        ErrorMessage.Builder builder = ErrorMessage.newBuilder()
                                                   .setLocation(client)
                                                   .setErrorCode(errorCategory.errorCode());
        if (t != null) {
            builder.setMessage(extractMessage(t));
            builder.addDetails(extractMessage(t));
            while (t.getCause() != null) {
                t = t.getCause();
                builder.addDetails(extractMessage(t));
            }
        }
        return builder.build();
    }

    private static String extractMessage(Throwable t) {
        return t.getMessage() == null ? t.getClass().getName() : t.getMessage();
    }
}
