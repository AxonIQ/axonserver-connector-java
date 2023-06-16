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

import io.axoniq.axonserver.connector.event.transformation.event.EventTransformationExecutor;
import io.axoniq.axonserver.connector.event.transformation.event.EventTransformer;
import io.axoniq.axonserver.connector.event.transformation.event.TransformableEventStream;
import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * {@link TransformableEventStream} implementation that is based on an {@link Iterable}.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public class TransformableEventIterable implements TransformableEventStream {

    private final Iterable<EventWithToken> events;

    /**
     * Constructs an instance based on the specified {@link Iterable}
     * @param events the {@link Iterable} of {@link EventWithToken}s used to retrieve events
     */
    public TransformableEventIterable(Iterable<EventWithToken> events) {
        this.events = events;
    }

    @Override
    public TransformableEventStream filter(Predicate<EventWithToken> predicate) {
        return new TransformableEventIterable(() -> new Iterator<EventWithToken>() {
            private final Iterator<EventWithToken> iterator = events.iterator();
            private final AtomicReference<EventWithToken> next = new AtomicReference<>();

            {   //prefetch the initial result
                nextMatchingThePredicate().ifPresent(next::set);
            }

            @Override
            public boolean hasNext() {
                return next.get() != null;
            }

            @Override
            public EventWithToken next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("");
                }
                EventWithToken current = next.get();
                next.set(nextMatchingThePredicate().orElse(null));
                return current;
            }

            private Optional<EventWithToken> nextMatchingThePredicate() {
                while (iterator.hasNext()) {
                    EventWithToken current = iterator.next();
                    if (predicate.test(current)) {
                        return Optional.of(current);
                    }
                }
                return Optional.empty();
            }


        });
    }

    @Override
    public EventTransformationExecutor transform(String transformationDescription,
                                                 EventTransformer eventTransformer) {
        return new DefaultEventTransformationExecutor(transformationDescription, events, eventTransformer);
    }
}
