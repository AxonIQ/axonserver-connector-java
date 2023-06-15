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

/**
 * The Event Transformation details.
 *
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface EventTransformation {

    /**
     * Returns the identifier of the EventTransformation.
     * @return the identifier of the EventTransformation.
     */
    String id();

    /**
     * Returns the description of the EventTransformation.
     * @return the description of the EventTransformation.
     */
    String description();

    /**
     * Each transformation action registered in the transformation has its own incremental sequence.
     * An action could represent the deletion of an Event, or its replacement.
     * This method returns the sequence of the last transformation action registered in the EventTransformation.
     * @return the last sequence of the EventTransformation.
     */
    Long lastSequence();

    /**
     * Returns the state of the EventTransformation.
     * @return the state of the EventTransformation.
     */
    State state();

    /**
     * The state of the transformation
     */
    enum State {

        /**
         * A transformation is in ACTIVE state when it is still open to accept new transformation actions.
         * A new transformation is created in ACTIVE state.
         */
        ACTIVE,

        /**
         * A transformation is in APPLYING state when the process to apply the transformation actions against the event
         * store is in progress. In this state, the transformation cannot accept any further transformation action.
         */
        APPLYING,

        /**
         * A transformation is in APPLIED state when all the transformation actions registered in it have been applied
         * to the event store.
         */
        APPLIED,

        /**
         * A transformation is in CANCELLED state when it has been discarded before to be applied. In this state, the
         * transformation cannot accept any further transformation action, and cannot be applied.
         */
        CANCELLED
    }
}
