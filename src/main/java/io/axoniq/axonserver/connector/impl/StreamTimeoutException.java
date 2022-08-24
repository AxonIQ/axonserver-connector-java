/*
 * Copyright (c) 2022. AxonIQ
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

/**
 * A {@link RuntimeException} to throw if reading from a stream results in a timeout.
 *
 * @author Mitchell Herrijgers
 * @since 4.5.6
 */
public class StreamTimeoutException extends RuntimeException {

    /**
     * Constructs a new instance of {@link StreamTimeoutException}.
     */
    public StreamTimeoutException() {
    }

    /**
     * Constructs a new instance of {@link StreamTimeoutException} with a detailed description of the cause of the
     * exception
     *
     * @param message the message describing the cause of the exception.
     */
    public StreamTimeoutException(String message) {
        super(message);
    }
}
