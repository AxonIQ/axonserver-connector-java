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
