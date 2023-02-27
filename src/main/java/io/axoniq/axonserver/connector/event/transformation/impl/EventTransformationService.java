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

package io.axoniq.axonserver.connector.event.transformation.impl;

import io.axoniq.axonserver.connector.event.transformation.EventTransformation;
import io.axoniq.axonserver.grpc.event.Event;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface EventTransformationService {

    CompletableFuture<Iterable<EventTransformation>> transformations();

    CompletableFuture<EventTransformation> transformationById(String id);

    CompletableFuture<String> newTransformation(String description);

    TransformationStream transformationStream(String transformationId);

    CompletableFuture<Void> startApplying(String transformationId, Long sequence);

    CompletableFuture<Void> cancel(String transformationId);

    CompletableFuture<Void> startCompacting();

    interface TransformationStream {

        CompletableFuture<Void> deleteEvent(long token, long sequence);

        CompletableFuture<Void> replaceEvent(long token, Event event, long sequence);

        void complete();

        void onCompletedByServer(Consumer<Optional<Throwable>> onCompleted);
    }
}
