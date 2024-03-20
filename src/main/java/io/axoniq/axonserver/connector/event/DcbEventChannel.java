/*
 * Copyright (c) 2020-2024. AxonIQ
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
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.ConsistencyCondition;
import io.axoniq.axonserver.grpc.event.dcb.ServerSentEvent;
import io.axoniq.axonserver.grpc.event.dcb.StreamQuery;
import io.axoniq.axonserver.grpc.event.dcb.TaggedEvent;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Communication channel for Event related interaction with Axon Server based on Dynamic Consistency Boundaries
 * concept.
 *
 * @author Milan Savic
 * @since 2024.1.0
 */
public interface DcbEventChannel {

    /**
     * Starts a new transaction to append events.
     *
     * @return the transaction reference onto which to register events to append along with the condition
     */
    AppendTransaction startTransaction();

    /**
     * Provides a finite stream of events.
     *
     * @param query the query used to filter events
     * @return a finite stream of events
     */
    ResultStream<ServerSentEvent> events(StreamQuery query);

    /**
     * Provides operations to interact with a Transaction to append events and a condition onto the Event Store.
     */
    interface AppendTransaction {

        /**
         * Sets the Consistency Condition for the transaction. Axon Server will validate this condition against the
         * Event Store and based on the validation outcome will accept or reject the transaction.
         *
         * @param condition the Consistency Condition used to validate the Transaction
         * @return this Transaction for fluency
         */
        AppendTransaction condition(ConsistencyCondition condition);

        /**
         * Appends this {@code taggedEvent} to this transaction.
         *
         * @param taggedEvent the event to be appended
         * @return this Transaction for fluency
         */
        AppendTransaction append(TaggedEvent taggedEvent);

        /**
         * Appends all events from the collection of {@code taggedEvents} to this transaction
         *
         * @param taggedEvents the collection of events to be appended
         * @return this Transaction for fluency
         */
        default AppendTransaction appendAll(Collection<TaggedEvent> taggedEvents) {
            taggedEvents.forEach(this::append);
            return this;
        }

        /**
         * Commits this Transaction.
         *
         * @return the future that completes once the Axon Server commits this Transaction
         */
        CompletableFuture<AppendEventsResponse> commit();

        /**
         * Rolls back the transaction.
         */
        void rollback();
    }
}
