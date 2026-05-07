/*
 * Copyright (c) 2020-2026. AxonIQ
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

package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.dcb.SnapshottedSourceEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.SnapshottedSourceRequest;

/**
 * Communication channel for sourcing events from Axon Server using the Dynamic Consistency Boundaries concept,
 * prefixed by the latest matching snapshot if one is available for the given snapshot key.
 *
 * @author Milan Savic
 * @since 2026.0.0
 */
public interface SnapshottedDcbEventChannel {

    /**
     * Provides a finite stream of events used to event-source a model, optionally prefixed by the latest matching
     * snapshot. If a snapshot exists for the {@link SnapshottedSourceRequest#getSnapshotKey() snapshot key} provided
     * in the {@code request}, that snapshot is emitted as the first response and events are sourced starting from
     * the sequence following the snapshot. If no snapshot exists, events are sourced from the beginning.
     *
     * @param request the request used to look up the snapshot and filter out events for sourcing
     * @return the response containing optionally a snapshot, events to source a model and a consistency marker to be
     * used when trying to append new events to the event store
     */
    ResultStream<SnapshottedSourceEventsResponse> source(SnapshottedSourceRequest request);
}
