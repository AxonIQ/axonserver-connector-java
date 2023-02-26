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
import java.util.function.Consumer;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface ActiveTransformation {

    CompletableFuture<ActiveTransformation> append(Consumer<Appender> appenderConsumer);

    CompletableFuture<EventTransformation> startApplying();

    CompletableFuture<EventTransformation> cancel();

    interface Appender {

        Appender fireDeleteEvent(long token);

        CompletableFuture<Appender> deleteEvent(long token);

        Appender fireReplaceEvent(long token, Event replacement);

        CompletableFuture<Appender> replaceEvent(long token, Event replacement);
    }
}
