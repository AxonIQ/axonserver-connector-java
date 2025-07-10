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
import io.axoniq.axonserver.grpc.event.dcb.GetTagsResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetTailResponse;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.TaggedEvent;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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
     *                  condition against the  Event Store and based on the validation outcome will accept or reject the
     *                  transaction.
     * @return the transaction reference onto which to register events to append
     */
    AppendEventsTransaction startTransaction(ConsistencyCondition condition);


    /**
     * Appends a {@code Collection} of {@code taggedEvents} and returns the completableFuture which contains the
     * {@code AppendEventsResponse} when awaited.
     *
     * @param taggedEvents the collection of events to be appended
     * @return the future that completes once the Axon Server commits this Transaction, containing the append events
     * response as a result.
     */
    default CompletableFuture<AppendEventsResponse> append(Collection<TaggedEvent> taggedEvents) {
        if (taggedEvents == null || taggedEvents.isEmpty()) {
            return noEmptyEvents();
        }
        return this.startTransaction()
                   .appendAll(taggedEvents)
                   .commit();
    }

    /**
     * Appends a {@code Collection} of {@code taggedEvents} with {@code ConsistencyCondition} and returns the
     * completableFuture which contains the {@code AppendEventsResponse} when awaited.
     *
     * @param taggedEvents the collection of events to be appended
     * @param condition    the Consistency Condition used to validate the Transaction. Axon Server will validate this
     *                     condition against the  Event Store and based on the validation outcome will accept or reject
     *                     the transaction.
     * @return the future that completes once the Axon Server commits this Transaction, containing the append events
     * response as a result.
     */
    default CompletableFuture<AppendEventsResponse> append(Collection<TaggedEvent> taggedEvents,
                                                           ConsistencyCondition condition) {
        if (taggedEvents == null || taggedEvents.isEmpty()) {
            return noEmptyEvents();
        }
        return this.startTransaction(condition)
                   .appendAll(taggedEvents)
                   .commit();
    }

    /**
     * Appends variable number of {@code taggedEvents} with {@code ConsistencyCondition} and returns the
     * completableFuture which contains the {@code AppendEventsResponse} when awaited.
     *
     * @param taggedEvents the 1..n of events to be appended
     * @param condition    the Consistency Condition used to validate the Transaction. Axon Server will validate this
     *                     condition against the  Event Store and based on the validation outcome will accept or reject
     *                     the transaction.
     * @return the future that completes once the Axon Server commits this Transaction, containing the append events
     * response as a result.
     */
    default CompletableFuture<AppendEventsResponse> append(ConsistencyCondition condition,
                                                           TaggedEvent... taggedEvents) {
        return this.append(Arrays.asList(taggedEvents), condition);
    }

    /**
     * Appends variable number of {@code taggedEvents} and returns the completableFuture which contains the
     * {@code AppendEventsResponse} when awaited.
     *
     * @param taggedEvents the 1..n of events to be appended
     * @return the future that completes once the Axon Server commits this Transaction, containing the append events
     * response as a result.
     */
    default CompletableFuture<AppendEventsResponse> append(TaggedEvent... taggedEvents) {
        return this.append(Arrays.asList(taggedEvents));
    }

    private static CompletableFuture<AppendEventsResponse> noEmptyEvents() {
        return CompletableFuture.failedFuture(new IllegalArgumentException("taggedEvents must not be null or empty"));
    }

    /**
     * Opens an infinite stream of events, for sequentially consuming events from Axon Server, using the given
     * {@code request} and keeping a local buffer of 64 events. When consuming 16 events, the client will request
     * additional events to keep the buffer filled.
     * <p>
     * The stream of events starts immediately upon the invocation of this method, making the first events available for
     * consumption as soon as they have arrived from Axon Server.
     * </p>
     *
     * @param request the request used to filter events
     * @return an infinite stream of events
     * @see #stream(StreamEventsRequest, int) to configure the buffer size
     */
    default ResultStream<StreamEventsResponse> stream(StreamEventsRequest request) {
        return stream(request, 64);
    }

    /**
     * Opens an infinite stream of events, for sequentially consuming events from Axon Server, using the given
     * {@code request} and keeping a local buffer of {@code bufferSize}. When consuming 1/8th of the buffer size (with a
     * minimum of 16), the client will request additional events to keep the buffer filled.
     * <p>
     * A value for {@code bufferSize} smaller than 64 events will result in a buffer of 64.
     * </p>
     * <p>
     * The stream of events starts immediately upon the invocation of this method, making the first events available for
     * consumption as soon as they have arrived from Axon Server.
     * </p>
     *
     * @param request    the request used to filter events
     * @param bufferSize the number of events to buffer locally
     * @return an infinite stream of events
     * @see #stream(StreamEventsRequest, int, int) to configure the refill frequency
     */
    default ResultStream<StreamEventsResponse> stream(StreamEventsRequest request, int bufferSize) {
        return stream(request, bufferSize, Math.max(bufferSize >> 3, 16));
    }

    /**
     * Opens an infinite stream of events, for sequentially consuming events from Axon Server, using the given
     * {@code request} and keeping a local buffer of {@code bufferSize}, which is refilled after consuming
     * {@code refillBatch} events.
     * <p>
     * A value for {@code bufferSize} smaller than 64 events will result in a buffer of 64. A value for
     * {@code refillBatch} smaller than 16 will result in a refill batch of 16. A value larger than the
     * {@code bufferSize} will be reduced to match the given {@code bufferSize}. While this will work, it is not
     * recommended. The {@code refillBatch} should be sufficiently small to allow for a constant flow of events to
     * consume.
     * </p>
     * <p>
     * The stream of events starts immediately upon the invocation of this method, making the first events available for
     * consumption as soon as they have arrived from Axon Server.
     * </p>
     *
     * @param request     the request used to filter events
     * @param bufferSize  the number of events to buffer locally
     * @param refillBatch the number of events to be consumed prior to refilling the buffer
     * @return an infinite stream of events
     */
    ResultStream<StreamEventsResponse> stream(StreamEventsRequest request, int bufferSize, int refillBatch);

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
     * @param sequence the sequence of an event
     * @return the tags fo the event
     */
    CompletableFuture<GetTagsResponse> tagsFor(long sequence);

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
