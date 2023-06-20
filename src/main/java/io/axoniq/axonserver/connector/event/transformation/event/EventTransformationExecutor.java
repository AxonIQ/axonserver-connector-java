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

import io.axoniq.axonserver.connector.event.transformation.EventTransformation;
import io.axoniq.axonserver.connector.event.transformation.EventTransformationChannel;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Executes the Transformation by invoking APPLY operation.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public interface EventTransformationExecutor {

    /**
     * Executes the Transformation by invoking APPLY operation.
     *
     * @param channelSupplier supplies the {@link EventTransformationChannel}
     * @return a {@link CompletableFuture} to indicate when the APPLY process has been accepted
     */
    CompletableFuture<EventTransformation> execute(Supplier<EventTransformationChannel> channelSupplier);
}
