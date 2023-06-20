/*
 * Copyright (c) 2020-2023. AxonIQ
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

package io.axoniq.axonserver.connector.event.transformation;

import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * Appends the transformation actions.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public interface Appender {

    /**
     * Appends to the transformation the request to delete the event with the given token.
     *
     * @param token the token of the event to be deleted
     * @return a {@link CompletableFuture} of the Appender, used for composition
     */
    CompletableFuture<Appender> deleteEvent(long token);

    /**
     * Appends to the transformation the request to replace the event with the given token.
     *
     * @param token       the token of the event to be replaced
     * @param replacement the new event used to replace the original one
     * @return a {@link CompletableFuture} of the Appender, used for composition
     */
    CompletableFuture<Appender> replaceEvent(long token, Event replacement);
}
