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

package io.axoniq.axonserver.connector.event.transformation.impl.grpc;

import io.axoniq.axonserver.connector.event.transformation.EventTransformation;
import io.axoniq.axonserver.grpc.event.Transformation;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class GrpcTransformation implements EventTransformation {

    private final Transformation transformation;

    public GrpcTransformation(Transformation transformation) {
        this.transformation = transformation;
    }

    @Override
    public String id() {
        return transformation.getTransformationId().getId();
    }

    @Override
    public String description() {
        return transformation.getDescription();
    }

    @Override
    public Long lastSequence() {
        return transformation.getSequence();
    }

    @Override
    public State state() {
        return State.valueOf(transformation.getState().name());
    }
}
