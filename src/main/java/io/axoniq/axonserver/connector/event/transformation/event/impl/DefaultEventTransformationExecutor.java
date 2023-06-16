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

package io.axoniq.axonserver.connector.event.transformation.event.impl;

import io.axoniq.axonserver.connector.event.transformation.Appender;
import io.axoniq.axonserver.connector.event.transformation.EventTransformation;
import io.axoniq.axonserver.connector.event.transformation.EventTransformationChannel;
import io.axoniq.axonserver.connector.event.transformation.event.EventTransformationExecutor;
import io.axoniq.axonserver.connector.event.transformation.event.EventTransformer;
import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public class DefaultEventTransformationExecutor implements EventTransformationExecutor {

    private final String transformationDescription;
    private final Iterable<EventWithToken> events;
    private final EventTransformer eventTransformer;

    public DefaultEventTransformationExecutor(String transformationDescription,
                                              Iterable<EventWithToken> events,
                                              EventTransformer eventTransformer) {
        this.transformationDescription = transformationDescription;
        this.events = events;
        this.eventTransformer = eventTransformer;
    }

    @Override
    public CompletableFuture<EventTransformation> execute(Supplier<EventTransformationChannel> channelSupplier) {
        return channelSupplier.get()
                              .transform(transformationDescription, this::transform);
    }

    private void transform(Appender appender) {
        events.forEach(event -> eventTransformer.accept(event, appender));
    }
}
