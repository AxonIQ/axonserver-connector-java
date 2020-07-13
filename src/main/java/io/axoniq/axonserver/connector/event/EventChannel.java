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

import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * Communication channel for Event related interactions with AxonServer
 */
public interface EventChannel {

    /**
     * Starts a new transaction to append events.
     *
     * @return The transaction reference onto which to register events to append
     */
    AppendEventsTransaction startAppendEventsTransaction();

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
     * @param aggregateId The identifier of the aggregate to find the sequence number for
     *
     * @return a CompletableFuture providing the sequence number found, or resolving to {@code null} when no event was
     * found.
     */
    CompletableFuture<Long> findHighestSequence(String aggregateId);

    /**
     * Open an EventStream, for sequentially consuming events from AxonServer, starting
     * at given {@code token} and keeping a local buffer of {@code bufferSize}. When
     * consuming 1/8th of the buffer size (with a minimum of 16), the client will request additional
     * messages to keep the buffer filled.
     * <p>
     * A value for {@code bufferSize} smaller than 64 items will result in a buffer of 64.
     * <p>
     * The stream of Events starts immediately upon the invocation of this method, making the first messages available
     * for consumption as soon as they have arrived from AxonServer.
     *
     * @param token      The token representing the position to start the stream, or {@code -1} to start from beginning
     * @param bufferSize The number of events to buffer locally
     *
     * @return the Stream for consuming the requested Events
     * @see #openStream(long, int, int) to configure the refill frequency.
     */
    default EventStream openStream(long token, int bufferSize) {
        return openStream(token, bufferSize, Math.max(bufferSize >> 3, 16));
    }

    /**
     * Open an EventStream, for sequentially consuming events from AxonServer, starting
     * at given {@code token} and keeping a local buffer of {@code bufferSize}, which is refilled after consuming
     * {@code refillBatch} items.
     * <p>
     * A value for {@code bufferSize} smaller than 64 items will result in a buffer of 64.
     * A value for {@code refillBatch} smaller than 16 will result in a refill batch of 16. A value larger than the
     * bufferSize will be reduced to match the given {@code bufferSize}. While this will work, it is not recommended.
     * The refillBatch should be sufficiently small to allow for a constant flow of messages to consume.
     * <p>
     * The stream of Events starts immediately upon the invocation of this method, making the first messages available
     * for consumption as soon as they have arrived from AxonServer.
     *
     * @param token      The token representing the position to start the stream, or {@code -1} to start from beginning
     * @param bufferSize The number of events to buffer locally
     *
     * @return the Stream for consuming the requested Events
     * @see #openStream(long, int) to use a sensible default for refill batch value.
     */
    default EventStream openStream(long token, int bufferSize, int refillBatch) {
        return openStream(token, bufferSize, refillBatch, false);
    }

    /**
     * Open an EventStream, for sequentially consuming events from AxonServer, starting
     * at given {@code token} and keeping a local buffer of {@code bufferSize}, which is refilled after consuming
     * {@code refillBatch} items.
     * <p>
     * A value for {@code bufferSize} smaller than 64 items will result in a buffer of 64.
     * A value for {@code refillBatch} smaller than 16 will result in a refill batch of 16. A value larger than the
     * bufferSize will be reduced to match the given {@code bufferSize}. While this will work, it is not recommended.
     * The refillBatch should be sufficiently small to allow for a constant flow of messages to consume.
     * <p>
     * The stream of Events starts immediately upon the invocation of this method, making the first messages available
     * for consumption as soon as they have arrived from AxonServer.
     *
     * @param token      The token representing the position to start the stream, or {@code -1} to start from beginning
     * @param bufferSize The number of events to buffer locally
     *
     * @return the Stream for consuming the requested Events
     * @see #openStream(long, int) to use a sensible default for refill batch value.
     */
    EventStream openStream(long token, int bufferSize, int refillBatch, boolean forceReadFromLeader);

    /**
     * Opens a stream for consuming Events from a single aggregate, allowing the first event to be a Snapshot event.
     * <p>
     * Note that this method does not have any form of flow control. All messages are buffered locally. When expecting
     * large streams of events, consider using {@link #openAggregateStream(String, long, long)} to retrieve chunks of
     * the event stream instead.
     *
     * @param aggregateIdentifier The identifier of the Aggregate to load events for
     *
     * @return a stream of Events for the given Aggregate
     */
    default AggregateEventStream openAggregateStream(String aggregateIdentifier) {
        return openAggregateStream(aggregateIdentifier, true);
    }

    /**
     * Opens a stream for consuming Events from a single aggregate, with given {@code allowSnapshots} indicating
     * whether the first event may be a Snapshot event. When given {@code allowSnapshots} is {@code false}, this
     * method will return events starting at the first available sequence number of the aggregate (typically 0).
     * <p>
     * Note that this method does not have any form of flow control. All messages are buffered locally. When expecting
     * large streams of events, consider using {@link #openAggregateStream(String, long, long)} to retrieve chunks of
     * the event stream instead.
     *
     * @param aggregateIdentifier The identifier of the Aggregate to load events for
     * @param allowSnapshots      whether to allow a snapshot event as first event, or not
     *
     * @return a stream of Events for the given Aggregate
     */
    AggregateEventStream openAggregateStream(String aggregateIdentifier, boolean allowSnapshots);

    /**
     * Opens a stream for consuming Events from a single aggregate, starting with the given {@code initialSequence}.
     * This method will not return a snapshot event as first event.
     * <p>
     * Note that this method does not have any form of flow control. All messages are buffered locally. When expecting
     * large streams of events, consider using {@link #openAggregateStream(String, long, long)} to retrieve chunks of
     * the event stream instead.
     *
     * @param aggregateIdentifier The identifier of the Aggregate to load events for
     * @param initialSequence     The sequence number of the first event to return
     *
     * @return a stream of Events for the given Aggregate
     */
    default AggregateEventStream openAggregateStream(String aggregateIdentifier, long initialSequence) {
        return openAggregateStream(aggregateIdentifier, initialSequence, 0);
    }

    /**
     * Opens a stream for consuming Events from a single aggregate, starting with the given {@code initialSequence}
     * until the given {@code maxSequence}. This method will not return a snapshot event as first event.
     * <p>
     * Note: a {@code maxSequence} of {@code 0} will result in all events after the initial sequence to be returned.
     *
     * @param aggregateIdentifier The identifier of the Aggregate to load events for
     * @param initialSequence     The sequence number of the first event to return
     * @param maxSequence         The sequence number of the last event to return
     *
     * @return a stream of Events for the given Aggregate
     */
    AggregateEventStream openAggregateStream(String aggregateIdentifier, long initialSequence, long maxSequence);

    /**
     * Store the given {@code snapshotEvent}.
     *
     * @param snapshotEvent The event to store
     *
     * @return a CompletableFuture providing the confirmation upon storage of the snapshot
     */
    CompletableFuture<Confirmation> appendSnapshot(Event snapshotEvent);

    /**
     * Loads snapshot events for the given {@code aggregateIdentifier} with sequence number between
     * {@code initialSequence} and {@code maxSequence} (inclusive), returning at most {@code maxResults} number of
     * snapshots.
     * <p>
     * Note that snapshot events are returned in reverse order of their sequence number.
     *
     * @param aggregateIdentifier The identifier of the Aggregate to retrieve snapshots for
     * @param initialSequence     The lowest sequence number of snapshots to return
     * @param maxSequence         The highest sequence number of snapshots to return
     * @param maxResults          The maximum allowed number of snapshots to return
     *
     * @return a stream containing snapshot events within the requested bounds.
     */
    AggregateEventStream loadSnapshots(String aggregateIdentifier, long initialSequence, long maxSequence, int maxResults);

    /**
     * Loads snapshot events for the given {@code aggregateIdentifier} with sequence number lower or equal to
     * {@code maxSequence}, returning at most {@code maxResults} number of snapshots.
     * <p>
     * Note that snapshot events are returned in reverse order of their sequence number.
     *
     * @param aggregateIdentifier The identifier of the Aggregate to retrieve snapshots for
     * @param maxSequence         The highest sequence number of snapshots to return
     * @param maxResults          The maximum allowed number of snapshots to return
     *
     * @return a stream containing snapshot events within the requested bounds.
     */
    default AggregateEventStream loadSnapshots(String aggregateIdentifier, long maxSequence, int maxResults) {
        return loadSnapshots(aggregateIdentifier, 0, maxSequence, maxResults);
    }

    /**
     * Loads the snapshot events for the given {@code aggregateIdentifier} with the highest sequence number.
     * <p>
     * Note that the returned stream may not yield any result when there are no snapshots available.
     *
     * @param aggregateIdentifier The identifier of the Aggregate to retrieve snapshots for
     *
     * @return a stream containing the most recent snapshot event.
     */
    default AggregateEventStream loadSnapshot(String aggregateIdentifier) {
        return loadSnapshots(aggregateIdentifier, 0, Long.MAX_VALUE, 1);
    }

    /**
     * Retrieves the Token referring to the most recent Event in the Event store. Using this token to open a stream will
     * only yield events that were appended after execution of this call.
     *
     * @return a completable future resolving the Token of the last (i.e. most recent) event
     */
    CompletableFuture<Long> getLastToken();

    /**
     * Retrieves the Token referring to the first Event in the Event store. Using this token to open a stream will
     * yield all events that available in the event store.
     *
     * @return a completable future resolving the Token of the first (i.e. oldest) event
     */
    CompletableFuture<Long> getFirstToken();

    /**
     * Retrieves the Token referring to the first Event in the Event store with a timestamp on or after given
     * {@code instant}. Using this token to open a stream will
     * yield all events with a timestamp starting at that instance, but may also yield events that were created before
     * this {@code instant}, whose append transaction was completed after the {@code instant}.
     *
     * @return a completable future resolving the Token of the first (i.e. oldest) event with timestamp on or after
     * given {@code instant}
     */
    CompletableFuture<Long> getTokenAt(long instant);
}
