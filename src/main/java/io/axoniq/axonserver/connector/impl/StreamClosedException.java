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

/**
 * A {@link RuntimeException} to throw if a stream is closed.
 */
public class StreamClosedException extends RuntimeException {

    /**
     * Constructs a new {@link StreamClosedException} using the given {@code cause}.
     *
     * @param cause the {@link Throwable} further defining this {@link StreamClosedException}
     */
    public StreamClosedException(Throwable cause) {
        super(cause);
    }
}
