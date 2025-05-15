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
import io.axoniq.axonserver.grpc.event.dcb.GetHeadResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetSequenceAtResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetTagsRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetTagsResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetTailResponse;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.TaggedEvent;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Communication channel for Event related interaction with Axon Server based on the Dynamic Consistency Boundaries
 * concept.
 *
 * @author Milan Savic
 * @since 2025.1.0
 */
public interface DcbEventChannel {

    /**
     * Starts a new transaction to unconditionally append events.
     *
     * @return the transaction reference onto which to register events to append
     */
    AppendEventsTransaction startTransaction();

    /**
     * Starts a new transaction to conditionally append events.
     *
     * @param condition the Consistency Condition used to validate the Transaction. Axon Server will validate this
     *                  condition against the  Event Store and based on the validation outcome will accept or reject
     *                  the transaction.
     * @return the transaction reference onto which to register events to append
     */
    AppendEventsTransaction startTransaction(ConsistencyCondition condition);

    /**
     * Provides an infinite stream of events.
     *
     * @param request the request used to filter events
     * @return an infinite stream of events
     */
    ResultStream<StreamEventsResponse> stream(StreamEventsRequest request);

    /**
     * Provides a finite stream of events used to eventsource a model.
     *
     * @param request the query used to filter out events for sourcing
     * @return the response containing events to source a model and a consistency marker to be used when trying to
     * append new events to the event store
     */
    ResultStream<SourceEventsResponse> source(SourceEventsRequest request);

    /**
     * Provides tags for an event at the given global sequence.
     *
     * @param request the sequence of an event
     * @return the tags fo the event
     */
    CompletableFuture<GetTagsResponse> tagsFor(GetTagsRequest request);

    /**
     * Provides the head of the event store.
     *
     * @return the head of the event store
     */
    CompletableFuture<GetHeadResponse> head();

    /**
     * Provides the tail of the event store.
     *
     * @return the tail of the event store
     */
    CompletableFuture<GetTailResponse> tail();

    /**
     * Provides the sequence number of the event closest to the given timestamp. If the timestamp is before the first
     * event, returns the tail sequence. If the timestamp is after the last event, returns the head sequence.
     *
     * @param timestamp the timestamp containing the timestamp to find the sequence for
     * @return the sequence at the given timestamp
     */
    CompletableFuture<GetSequenceAtResponse> getSequenceAt(Instant timestamp);

    /**
     * Provides operations to interact with a Transaction to append events and a condition onto the Event Store.
     */
    interface AppendEventsTransaction {

        /**
         * Appends this {@code taggedEvent} to this transaction.
         *
         * @param taggedEvent the event to be appended
         * @return this Transaction for fluency
         */
        AppendEventsTransaction append(TaggedEvent taggedEvent);

        /**
         * Appends all events from the collection of {@code taggedEvents} to this transaction
         *
         * @param taggedEvents the collection of events to be appended
         * @return this Transaction for fluency
         */
        default AppendEventsTransaction appendAll(Collection<TaggedEvent> taggedEvents) {
            taggedEvents.forEach(this::append);
            return this;
        }

        /**
         * Commits this Transaction.
         *
         * @return the future that completes once the Axon Server commits this Transaction, containing the append events
         * response as a result.
         */
        CompletableFuture<AppendEventsResponse> commit();

        /**
         * Rolls back the transaction.
         */
        void rollback();
    }
}
