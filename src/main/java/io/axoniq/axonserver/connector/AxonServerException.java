/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector;

import io.axoniq.axonserver.grpc.ErrorMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Exception that represents an Error reported by AxonServer.
 */
public class AxonServerException extends RuntimeException {

    private final ErrorCategory errorCategory;
    private final String location;
    private final List<String> details;

    /**
     * Initialize the exception for the error reported in the given {@code errorMessage}.
     *
     * @param errorMessage the ErrorMessage provided by AxonServer to describe the error
     */
    public AxonServerException(ErrorMessage errorMessage) {
        this(ErrorCategory.getFromCode(errorMessage.getErrorCode()),
             errorMessage.getMessage(),
             errorMessage.getLocation(),
             errorMessage.getDetailsList(),
             null);
    }

    /**
     * Initialize the exception for an error in the given {@code errorCategory}, explained by the given {@code message},
     * which occurred in given {@code location}.
     *
     * @param errorCategory the category of error occurring
     * @param message       the message describing the exception
     * @param location      the location (AxonServer, client Id) where the error occurred
     */
    public AxonServerException(ErrorCategory errorCategory, String message, String location) {
        this(errorCategory, message, location, Collections.emptyList(), null);
    }

    /**
     * Initialize the exception for an error in the given {@code errorCategory}, explained by the given {@code message},
     * which occurred in given {@code location} and caused by the given {@code cause}.
     *
     * @param errorCategory the category of error occurring
     * @param message       the message describing the exception
     * @param location      the location (AxonServer, client Id) where the error occurred
     * @param cause         the underlying cause of the exception. May be {@code null}
     */
    public AxonServerException(ErrorCategory errorCategory, String message, String location, Throwable cause) {
        this(errorCategory, message, location, Collections.emptyList(), cause);
    }

    /**
     * Initialize the exception for an error in the given {@code errorCategory}, explained by the given {@code message}
     * and {@code details}, which occurred in given {@code location} and caused by the given {@code cause}.
     *
     * @param errorCategory the category of error occurring
     * @param message       the message describing the exception
     * @param location      the location (AxonServer, client Id) where the error occurred
     * @param details       a list of messages detailing underlying causes
     * @param cause         the underlying cause of the exception. May be {@code null}
     */
    public AxonServerException(ErrorCategory errorCategory, String message, String location, List<String> details,
                               Throwable cause) {
        super("[" + errorCategory.errorCode() + "] " + message, cause);
        this.errorCategory = errorCategory;
        this.location = location;
        this.details = Collections.unmodifiableList(new ArrayList<>(details));
    }

    /**
     * Returns the category of the error that caused this exception.
     *
     * @return the category of the error that caused this exception
     */
    public ErrorCategory getErrorCategory() {
        return errorCategory;
    }

    /**
     * Returns the list of messages detailing the underlying cause. The returned list is unmodifiable.
     *
     * @return the list of messages detailing the underlying cause
     */
    public List<String> getDetails() {
        return details;
    }

    /**
     * Returns the identifier of the location where the exception occurred.
     *
     * @return the identifier of the location where the exception occurred
     */
    public String getLocation() {
        return location;
    }
}
