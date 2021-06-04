/*
 * Copyright (c) 2020. AxonIQ
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

package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Communication channel for Event related interactions with AxonServer.
 */
public interface EventChannel {

    /**
     * Starts a new transaction to append events.
     *
     * @return The transaction reference onto which to register events to append
     */
    AppendEventsTransaction startAppendEventsTransaction();

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
     * Convenience method to cancel the scheduled event with given {@code scheduleToken} and reschedule the given {@code
     * event} to be published after given {@code triggerDuration}. Is effectively the same as cancelling and scheduling
     * in separate calls, but this call requires only a single round-trip to the server.
     *
     * @param scheduleToken   the token of the event to cancel
     * @param triggerDuration the point amount of time to wait to publish the event
     * @param event           the event to publish
     * @return a future reference to the token for the new schedule
     */
    default CompletableFuture<String> reschedule(String scheduleToken, Duration triggerDuration, Event event) {
        return reschedule(scheduleToken, Instant.now().plus(triggerDuration), event);
    }

    /**
     * Convenience method to cancel the scheduled event with given {@code scheduleToken} and reschedule the given {@code
     * event} to be published at given {@code scheduleTime}. Is effectively the same as cancelling and scheduling in
     * separate calls, but this call requires only a single round-trip to the server.
     *
     * @param scheduleToken the token of the event to cancel
     * @param scheduleTime  the point in time to publish the new event
     * @param event         the event to publish
     * @return a future reference to the token for the new schedule
     */
    CompletableFuture<String> reschedule(String scheduleToken, Instant scheduleTime, Event event);

    /**
     * Append the given {@code events} to the Event Store. Prior to starting, the {@link
     * #startAppendEventsTransaction()} method should be invoked
     *
     * @param events the {@link Event}s to append to the Event Store
     * @return a {@link CompletableFuture} resolving the confirmation of the successful processing of the append
     * transaction
     */
    default CompletableFuture<Confirmation> appendEvents(Event... events) {
        AppendEventsTransaction tx = startAppendEventsTransaction();
        for (Event event : events) {
            tx.appendEvent(event);
        }
        return tx.commit();
    }

    /**
     * Find the highest sequence number (i.e. the sequence number of the most recent event) for an aggregate with given
     * {@code aggregateId}.
     * <p>
     * If no events for an aggregate with given identifier have been found, the returned CompletableFuture will resolve
     * to a {@code null} value.
     *
     * @param aggregateId the identifier of the aggregate to find the sequence number for
     * @return a CompletableFuture providing the sequence number found, or resolving to {@code null} when no event was
     * found.
     */
    CompletableFuture<Long> findHighestSequence(String aggregateId);

    /**
     * Opens an EventStream, for sequentially consuming events from AxonServer, starting at given {@code token} and
     * keeping a local buffer of {@code bufferSize}. When consuming 1/8th of the buffer size (with a minimum of 16), the
     * client will request additional messages to keep the buffer filled.
     * <p>
     * A value for {@code bufferSize} smaller than 64 items will result in a buffer of 64.
     * <p>
     * The stream of Events starts immediately upon the invocation of this method, making the first messages available
     * for consumption as soon as they have arrived from AxonServer.
     *
     * @param token      the token representing the position to start the stream, or {@code -1} to start from beginning
     * @param bufferSize the number of events to buffer locally
     * @return the Stream for consuming the requested Events
     * @see #openStream(long, int, int) to configure the refill frequency.
     */
    default EventStream openStream(long token, int bufferSize) {
        return openStream(token, bufferSize, Math.max(bufferSize >> 3, 16));
    }

    /**
     * Open an EventStream, for sequentially consuming events from AxonServer, starting at given {@code token} and
     * keeping a local buffer of {@code bufferSize}, which is refilled after consuming {@code refillBatch} items.
     * <p>
     * A value for {@code bufferSize} smaller than 64 items will result in a buffer of 64. A value for {@code
     * refillBatch} smaller than 16 will result in a refill batch of 16. A value larger than the {@code bufferSize} will
     * be reduced to match the given {@code bufferSize}. While this will work, it is not recommended. The {@code
     * refillBatch} should be sufficiently small to allow for a constant flow of messages to consume.
     * <p>
     * The stream of Events starts immediately upon the invocation of this method, making the first messages available
     * for consumption as soon as they have arrived from AxonServer.
     *
     * @param token       the token representing the position to start the stream, or {@code -1} to start from
     *                    beginning
     * @param bufferSize  the number of events to buffer locally
     * @param refillBatch the number of events to be consumed prior to refilling the buffer
     * @return the Stream for consuming the requested Events
     * @see #openStream(long, int) to use a sensible default for refill batch value.
     */
    default EventStream openStream(long token, int bufferSize, int refillBatch) {
        return openStream(token, bufferSize, refillBatch, false);
    }

    /**
     * Open an EventStream, for sequentially consuming events from AxonServer, starting at given {@code token} and
     * keeping a local buffer of {@code bufferSize}, which is refilled after consuming {@code refillBatch} items. The
     * {@code forceReadFromLeader} parameter can be used to enforce this stream to read events from the RAFT leader.
     * <p>
     * A value for {@code bufferSize} smaller than 64 items will result in a buffer of 64. A value for {@code
     * refillBatch} smaller than 16 will result in a refill batch of 16. A value larger than the {@code bufferSize} will
     * be reduced to match the given {@code bufferSize}. While this will work, it is not recommended. The {@code
     * refillBatch} should be sufficiently small to allow for a constant flow of messages to consume.
     * <p>
     * The stream of Events starts immediately upon the invocation of this method, making the first messages available
     * for consumption as soon as they have arrived from AxonServer.
     *
     * @param token               the token representing the position to start the stream, or {@code -1} to start from
     *                            beginning
     * @param bufferSize          the number of events to buffer locally
     * @param refillBatch         the number of events to be consumed prior to refilling the buffer
     * @param forceReadFromLeader a {@code boolean} defining whether Events <b>must</b> be read from the leader
     * @return the Stream for consuming the requested Events
     * @see #openStream(long, int) to use a sensible default for refill batch value.
     */
    EventStream openStream(long token, int bufferSize, int refillBatch, boolean forceReadFromLeader);

    /**
     * Opens a stream for consuming Events from a single aggregate, allowing the first event to be a Snapshot Event.
     * <p>
     * Note that this method does not have any form of flow control. All messages are buffered locally. When expecting
     * large streams of events, consider using {@link #openAggregateStream(String, long, long)} to retrieve chunks of
     * the event stream instead.
     *
     * @param aggregateIdentifier the identifier of the Aggregate to load events for
     * @return a stream of Events for the given Aggregate, for which the first event can be a Snapshot Event
     */
    default AggregateEventStream openAggregateStream(String aggregateIdentifier) {
        return openAggregateStream(aggregateIdentifier, true);
    }

    /**
     * Opens a stream for consuming Events from a single aggregate, with given {@code allowSnapshots} indicating whether
     * the first Event may be a Snapshot Event. When given {@code allowSnapshots} is {@code false}, this method will
     * return events starting at the first available sequence number of the aggregate (typically 0).
     * <p>
     * Note that this method does not have any form of flow control. All messages are buffered locally. When expecting
     * large streams of events, consider using {@link #openAggregateStream(String, long, long)} to retrieve chunks of
     * the event stream instead.
     *
     * @param aggregateIdentifier the identifier of the Aggregate to load events for
     * @param allowSnapshots      a {@code boolean} whether to allow a snapshot event as first event, or not
     * @return a stream of Events for the given Aggregate
     */
    AggregateEventStream openAggregateStream(String aggregateIdentifier, boolean allowSnapshots);

    /**
     * Opens a stream for consuming Events from a single aggregate, starting with the given {@code initialSequence}.
     * This method will not return a Snapshot Event as first event.
     * <p>
     * Note that this method does not have any form of flow control. All messages are buffered locally. When expecting
     * large streams of events, consider using {@link #openAggregateStream(String, long, long)} to retrieve chunks of
     * the event stream instead.
     *
     * @param aggregateIdentifier the identifier of the Aggregate to load events for
     * @param initialSequence     the sequence number of the first event to return
     * @return a stream of Events for the given Aggregate
     */
    default AggregateEventStream openAggregateStream(String aggregateIdentifier, long initialSequence) {
        return openAggregateStream(aggregateIdentifier, initialSequence, 0);
    }

    /**
     * Opens a stream for consuming Events from a single aggregate, starting with the given {@code initialSequence}
     * until the given {@code maxSequence}. This method will not return a Snapshot Event as first event.
     * <p>
     * Note: a {@code maxSequence} of {@code 0} will result in all events after the initial sequence to be returned.
     *
     * @param aggregateIdentifier the identifier of the Aggregate to load events for
     * @param initialSequence     the sequence number of the first event to return
     * @param maxSequence         the sequence number of the last event to return
     * @return a stream of Events for the given Aggregate
     */
    AggregateEventStream openAggregateStream(String aggregateIdentifier, long initialSequence, long maxSequence);

    /**
     * Store the given {@code snapshotEvent}.
     *
     * @param snapshotEvent the Snapshot Event to store
     * @return a CompletableFuture providing the confirmation upon storage of the snapshot
     */
    CompletableFuture<Confirmation> appendSnapshot(Event snapshotEvent);

    /**
     * Loads the Snapshot Event for the given {@code aggregateIdentifier} with the highest sequence number.
     * <p>
     * Note that the returned stream may not yield any result when there are no snapshots available.
     *
     * @param aggregateIdentifier the identifier of the Aggregate to retrieve snapshots for
     * @return a stream containing the most recent Snapshot Event
     */
    default AggregateEventStream loadSnapshot(String aggregateIdentifier) {
        return loadSnapshots(aggregateIdentifier, 0, Long.MAX_VALUE, 1);
    }

    /**
     * Loads Snapshot Events for the given {@code aggregateIdentifier} with sequence number lower or equal to {@code
     * maxSequence}, returning at most {@code maxResults} number of snapshots.
     * <p>
     * Note that Snapshot Events are returned in reverse order of their sequence number.
     *
     * @param aggregateIdentifier the identifier of the Aggregate to retrieve snapshots for
     * @param maxSequence         the highest sequence number of snapshots to return
     * @param maxResults          the maximum allowed number of snapshots to return
     * @return a stream containing Snapshot Events within the requested bounds
     */
    default AggregateEventStream loadSnapshots(String aggregateIdentifier, long maxSequence, int maxResults) {
        return loadSnapshots(aggregateIdentifier, 0, maxSequence, maxResults);
    }

    /**
     * Loads Snapshot Events for the given {@code aggregateIdentifier} with sequence number between {@code
     * initialSequence} and {@code maxSequence} (inclusive), returning at most {@code maxResults} number of snapshots.
     * <p>
     * Note that Snapshot Events are returned in reverse order of their sequence number.
     *
     * @param aggregateIdentifier the identifier of the Aggregate to retrieve snapshots for
     * @param initialSequence     the lowest sequence number of snapshots to return
     * @param maxSequence         the highest sequence number of snapshots to return
     * @param maxResults          the maximum allowed number of snapshots to return
     * @return a stream containing Snapshot Events within the requested bounds
     */
    AggregateEventStream loadSnapshots(String aggregateIdentifier,
                                       long initialSequence,
                                       long maxSequence,
                                       int maxResults);

    /**
     * Retrieves the Token referring to the most recent Event in the Event Store. Using this token to open a stream will
     * only yield events that were appended after execution of this call.
     *
     * @return a completable future resolving the Token of the last (i.e. most recent) event
     */
    CompletableFuture<Long> getLastToken();

    /**
     * Retrieves the Token referring to the first Event in the Event Store. Using this token to open a stream will yield
     * all events that available in the event store.
     *
     * @return a completable future resolving the Token of the first (i.e. oldest) event
     */
    CompletableFuture<Long> getFirstToken();

    /**
     * Retrieves the Token referring to the first Event in the Event Store with a timestamp on or after given {@code
     * instant}. Using this token to open a stream will yield all events with a timestamp starting at that instance, but
     * may also yield events that were created before this {@code instant}, whose append transaction was completed after
     * the {@code instant}.
     *
     * @return a completable future resolving the Token of the first (i.e. oldest) event with timestamp on or after
     * given {@code instant}
     */
    CompletableFuture<Long> getTokenAt(long instant);

    /**
     * Queries the Event Store for events using given {@code queryExpression}. The given {@code liveStream} indicates
     * whether the query should complete when the end of the Event Stream is reached, or if the query should continue
     * processing events as they are stored.
     *
     * @param queryExpression a valid Event Stream Query Language expression
     * @param liveStream      whether to continue processing live events
     * @return a ResultStream containing the query results
     */
    ResultStream<EventQueryResultEntry> queryEvents(String queryExpression, boolean liveStream);

    /**
     * Queries the Event Store for snapshot events using given {@code queryExpression}. The given {@code liveStream}
     * indicates whether the query should complete when the end of the Snapshot Event Stream is reached, or if the query
     * should continue processing snapshot events as they are stored.
     *
     * @param queryExpression a valid Event Stream Query Language expression
     * @param liveStream      whether to continue processing live snapshot events
     * @return a ResultStream containing the query results
     */
    ResultStream<EventQueryResultEntry> querySnapshotEvents(String queryExpression, boolean liveStream);
}
