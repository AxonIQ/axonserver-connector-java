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

public class MessageFactory {

    private MessageFactory() {

    }

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
