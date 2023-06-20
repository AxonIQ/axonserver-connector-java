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

package io.axoniq.axonserver.connector.event.transformation.event;

import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.transformation.event.impl.TransformableEventIterable;
import io.axoniq.axonserver.connector.event.transformation.event.stream.TokenRangeEvents;
import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public interface EventSources {

    static TransformableEventStream fromIterable(Iterable<EventWithToken> events) {
        return new TransformableEventIterable(events);
    }

    static TransformableEventStream range(Supplier<EventChannel> eventChannelSupplier, long first, long last) {
        return EventSources.fromIterable(new TokenRangeEvents(eventChannelSupplier, first, last));
    }
}
