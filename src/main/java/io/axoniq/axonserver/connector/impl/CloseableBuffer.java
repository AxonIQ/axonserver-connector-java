/*
 * Copyright (c) 2020-2022. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.grpc.ErrorMessage;

/**
 * A buffer that can be closed by the producing side.
 *
 * @param <T> the type of messages in this buffer
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 * @since 4.6.0
 */
public interface CloseableBuffer<T> extends CloseableReadonlyBuffer<T> {

    /**
     * Puts a message in this buffer.
     *
     * @param message the message to be put in this buffer
     */
    void put(T message);

    /**
     * Closes this buffer from the producing side.
     */
    void close();

    /**
     * Closes exceptionally this buffer from the producing side.
     *
     * @param errorMessage an error indicating why this buffer is closed
     */
    void closeExceptionally(ErrorMessage errorMessage);
}
