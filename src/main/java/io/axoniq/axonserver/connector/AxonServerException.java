/*
 * Copyright (c) 2020. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
     * @param errorMessage The ErrorMessage provided by AxonServer to describe the error
     */
    public AxonServerException(ErrorMessage errorMessage) {
        this(ErrorCategory.getFromCode(errorMessage.getErrorCode()), errorMessage.getMessage(), errorMessage.getLocation(), errorMessage.getDetailsList(), null);
    }

    /**
     * Initialize the exception for an error in given {@code errorCategory}, explained in given {@code message}, which
     * occurred in given {@code location}.
     *
     * @param errorCategory The category of error occurring
     * @param message       The message describing the exception
     * @param location      The location (AxonServer, client Id) where the error occurred
     */
    public AxonServerException(ErrorCategory errorCategory, String message, String location) {
        this(errorCategory, message, location, Collections.emptyList(), null);
    }

    /**
     * Initialize the exception for an error in given {@code errorCategory}, explained in given {@code message}, which
     * occurred in given {@code location} and caused by given {@code cause}.
     *
     * @param errorCategory The category of error occurring
     * @param message       The message describing the exception
     * @param location      The location (AxonServer, client Id) where the error occurred
     * @param cause         The underlying cause of the exception. May be {@code null}
     */
    public AxonServerException(ErrorCategory errorCategory, String message, String location, Throwable cause) {
        this(errorCategory, message, location, Collections.emptyList(), cause);
    }

    /**
     * Initialize the exception for an error in given {@code errorCategory}, explained in given {@code message} and
     * {@code details}, which occurred in given {@code location} and caused by given {@code cause}.
     *
     * @param errorCategory The category of error occurring
     * @param message       The message describing the exception
     * @param location      The location (AxonServer, client Id) where the error occurred
     * @param details       A list of messages detailing underlying causes
     * @param cause         The underlying cause of the exception. May be {@code null}
     */
    public AxonServerException(ErrorCategory errorCategory, String message, String location, List<String> details, Throwable cause) {
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
