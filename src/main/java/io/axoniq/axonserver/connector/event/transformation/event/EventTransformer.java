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

import io.axoniq.axonserver.connector.event.transformation.Appender;
import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.function.BiConsumer;

/**
 * Provides a way to transform a given event by invoking operations on the {@link Appender}.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public interface EventTransformer extends BiConsumer<EventWithToken, Appender> {

}
