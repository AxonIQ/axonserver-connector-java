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
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.event.dcb.AddTagsResponse;
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.ConsistencyCondition;
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.GetHeadResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetSequenceAtResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetTagsResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetTailResponse;
import io.axoniq.axonserver.grpc.event.dcb.RemoveTagsResponse;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.Tag;
import io.axoniq.axonserver.grpc.event.dcb.TaggedEvent;

import java.time.Duration;
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
     *                  condition against the  Event Store and based on the validation outcome will accept or reject the
     *                  transaction.
     * @return the transaction reference onto which to register events to append
     */
    AppendEventsTransaction startTransaction(ConsistencyCondition condition);

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
     * Assigns tags to the event identified by its sequence.
     *
     * @param sequence the sequence of an event
     * @param tags     the tags to add to the event
     * @return a CompletableFuture containing the response
     */
    CompletableFuture<AddTagsResponse> addTags(long sequence, Collection<Tag> tags);

    /**
     * Removes tags from the event identified by its sequence.
     *
     * @param sequence the sequence of an event
     * @param tags     the tags to remove from the event
     * @return a CompletableFuture containing the response
     */
    CompletableFuture<RemoveTagsResponse> removeTags(long sequence, Collection<Tag> tags);

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
     * Schedule the given {@code event} to be published after given {@code triggerDuration}. The returned value can be
     * used to cancel the schedule, or to reschedule the event to another time.
     *
     * @param triggerDuration the amount of time to wait to publish the event
     * @param event           the event to publish
     * @return a token used to cancel the schedule
     */
    default CompletableFuture<String> scheduleEvent(Duration triggerDuration, Event event) {
        return scheduleEvent(Instant.now().plus(triggerDuration), event);
    }

    /**
     * Schedule the given {@code event} to be published at given {@code scheduleTime}. The returned value can be used to
     * cancel the schedule, or to reschedule the event to another time.
     *
     * @param scheduleTime The scheduleTime at which to publish the event
     * @param event        The event to publish
     * @return a token used to cancel the schedule
     */
    CompletableFuture<String> scheduleEvent(Instant scheduleTime, Event event);

    /**
     * Cancels the scheduled publication of an event for which the given {@code scheduleToken} was returned.
     *
     * @param scheduleToken the token provided when scheduling the event to be cancelled
     * @return a future reference to the result of the instruction
     */
    CompletableFuture<InstructionAck> cancelSchedule(String scheduleToken);

    /**
     * Reschedules the scheduled event with the given {@code scheduleToken} and reschedule it
     * to be published after given {@code triggerDuration}. If no event is provided, the original event will be
     * rescheduled.
     *
     * @param scheduleToken   the token of the event to cancel
     * @param triggerDuration the point amount of time to wait to publish the event
     * @param event           an optional new event to publish
     * @return a future reference to the token for the new schedule
     */
    default CompletableFuture<String> reschedule(String scheduleToken, Duration triggerDuration, Event event) {
        return reschedule(scheduleToken, Instant.now().plus(triggerDuration), event);
    }

    /**
     * Reschedules the scheduled event with the given {@code scheduleToken} and reschedule it
     * to be published at given {@code scheduleTime}. If no event is provided, the original event will be
     * rescheduled.
     *
     * @param scheduleToken the token of the event to cancel
     * @param scheduleTime  the point in time to publish the event
     * @param event         an optional new event to publish
     * @return a future reference to the token for the new schedule
     */
    CompletableFuture<String> reschedule(String scheduleToken, Instant scheduleTime, Event event);

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
