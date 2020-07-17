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
