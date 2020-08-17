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

package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.event.AggregateEventStream;
import io.axoniq.axonserver.connector.event.AppendEventsTransaction;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.FutureStreamObserver;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.event.CancelScheduledEventRequest;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventSchedulerGrpc;
import io.axoniq.axonserver.grpc.event.EventStoreGrpc;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetFirstTokenRequest;
import io.axoniq.axonserver.grpc.event.GetLastTokenRequest;
import io.axoniq.axonserver.grpc.event.GetTokenAtRequest;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrRequest;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrResponse;
import io.axoniq.axonserver.grpc.event.RescheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleToken;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link EventChannel} implementation, serving as the event connection between AxonServer and a client application.
 */
public class EventChannelImpl extends AbstractAxonServerChannel implements EventChannel {

    private static final ReadHighestSequenceNrResponse UNKNOWN_HIGHEST_SEQ =
            ReadHighestSequenceNrResponse.newBuilder().setToSequenceNr(-1).build();
    private static final TrackingToken NO_TOKEN_AVAILABLE = TrackingToken.newBuilder().setToken(-1).build();

    private final EventStoreGrpc.EventStoreStub eventStore;
    private final EventSchedulerGrpc.EventSchedulerStub eventScheduler;
    private final Set<BufferedEventStream> buffers = ConcurrentHashMap.newKeySet();
    // guarded by -this-

    /**
     * Constructs a {@link EventChannelImpl}.
     *
     * @param executor a {@link ScheduledExecutorService} used to schedule reconnects of this channel
     * @param channel  the {@link AxonServerManagedChannel} used to form the connection with AxonServer
     */
    public EventChannelImpl(ScheduledExecutorService executor, AxonServerManagedChannel channel) {
        super(executor, channel);
        eventStore = EventStoreGrpc.newStub(channel);
        eventScheduler = EventSchedulerGrpc.newStub(channel);
    }

    @Override
    public synchronized void connect() {
        // there is no instruction stream for the events channel
    }

    @Override
    public void reconnect() {
        closeEventStreams();
    }

    @Override
    public void disconnect() {
        closeEventStreams();
    }

    private void closeEventStreams() {
        buffers.forEach(BufferedEventStream::close);
        buffers.clear();
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public AppendEventsTransaction startAppendEventsTransaction() {
        FutureStreamObserver<Confirmation> result = new FutureStreamObserver<>(null);
        StreamObserver<Event> clientStream = eventStore.appendEvent(result);
        return new AppendEventsTransactionImpl(clientStream, result);
    }

    @Override
    public CompletableFuture<String> scheduleEvent(Instant scheduleTime, Event event) {
        FutureStreamObserver<ScheduleToken> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while scheduling an Event. No response received from Server.",
                ""
        ));

        eventScheduler.scheduleEvent(ScheduleEventRequest.newBuilder().build(), responseObserver);
        return responseObserver.thenApply(ScheduleToken::getToken);
    }

    @Override
    public CompletableFuture<InstructionAck> cancelSchedule(String token) {
        FutureStreamObserver<InstructionAck> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling a scheduled Event. No response received from Server.",
                ""
        ));

        eventScheduler.cancelScheduledEvent(CancelScheduledEventRequest.newBuilder().setToken(token).build(),
                                            responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<String> reschedule(String scheduleToken, Instant scheduleTime, Event event) {
        FutureStreamObserver<ScheduleToken> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while rescheduling Event. No response received from Server.",
                ""
        ));

        eventScheduler.rescheduleEvent(RescheduleEventRequest.newBuilder()
                                                             .setToken(scheduleToken)
                                                             .setEvent(event)
                                                             .setInstant(scheduleTime.toEpochMilli())
                                                             .build(),
                                       responseObserver);
        return responseObserver.thenApply(ScheduleToken::getToken);
    }

    @Override
    public CompletableFuture<Long> findHighestSequence(String aggregateId) {
        FutureStreamObserver<ReadHighestSequenceNrResponse> result = new FutureStreamObserver<>(UNKNOWN_HIGHEST_SEQ);
        eventStore.readHighestSequenceNr(ReadHighestSequenceNrRequest.newBuilder()
                                                                     .setAggregateId(aggregateId)
                                                                     .build(),
                                         result);
        return result.thenApply(ReadHighestSequenceNrResponse::getToSequenceNr);
    }

    @Override
    public EventStream openStream(long token, int bufferSize, int refillBatch, boolean forceReadFromLeader) {
        BufferedEventStream buffer = new BufferedEventStream(token,
                                                             Math.max(64, bufferSize),
                                                             Math.max(16, Math.min(bufferSize, refillBatch)),
                                                             forceReadFromLeader);
        buffers.add(buffer);
        buffer.onCloseRequested(() -> buffers.remove(buffer));
        try {
            //noinspection ResultOfMethodCallIgnored
            eventStore.listEvents(buffer);
        } catch (Exception e) {
            buffers.remove(buffer);
            throw e;
        }
        buffer.enableFlowControl();
        return buffer;
    }

    @Override
    public AggregateEventStream openAggregateStream(String aggregateIdentifier, boolean allowSnapshots) {
        return doGetAggregateStream(GetAggregateEventsRequest.newBuilder()
                                                             .setAggregateId(aggregateIdentifier)
                                                             .setAllowSnapshots(allowSnapshots)
                                                             .build());
    }

    @Override
    public AggregateEventStream openAggregateStream(String aggregateIdentifier,
                                                    long initialSequence,
                                                    long maxSequence) {
        return doGetAggregateStream(GetAggregateEventsRequest.newBuilder()
                                                             .setAggregateId(aggregateIdentifier)
                                                             .setInitialSequence(initialSequence)
                                                             .setMaxSequence(maxSequence)
                                                             .build());
    }

    @Override
    public CompletableFuture<Confirmation> appendSnapshot(Event snapshotEvent) {
        FutureStreamObserver<Confirmation> result =
                new FutureStreamObserver<>(Confirmation.newBuilder().setSuccess(false).build());
        eventStore.appendSnapshot(snapshotEvent, result);
        return result;
    }

    @Override
    public AggregateEventStream loadSnapshots(String aggregateIdentifier,
                                              long initialSequence,
                                              long maxSequence,
                                              int maxResults) {
        BufferedAggregateEventStream buffer = new BufferedAggregateEventStream(maxResults);
        eventStore.listAggregateSnapshots(GetAggregateSnapshotsRequest.newBuilder()
                                                                      .setInitialSequence(initialSequence)
                                                                      .setMaxResults(maxResults)
                                                                      .setMaxSequence(maxSequence)
                                                                      .setAggregateId(aggregateIdentifier)
                                                                      .build(),
                                          buffer);
        return buffer;
    }

    @Override
    public CompletableFuture<Long> getLastToken() {
        FutureStreamObserver<TrackingToken> result = new FutureStreamObserver<>(NO_TOKEN_AVAILABLE);
        eventStore.getLastToken(GetLastTokenRequest.newBuilder().build(), result);
        return result.thenApply(TrackingToken::getToken);
    }

    @Override
    public CompletableFuture<Long> getFirstToken() {
        FutureStreamObserver<TrackingToken> result = new FutureStreamObserver<>(NO_TOKEN_AVAILABLE);
        eventStore.getFirstToken(GetFirstTokenRequest.newBuilder().build(), result);
        // older versions of AxonServer erroneously return -1 in certain cases. We assume 0 in such case.
        return result.thenApply(trackingToken -> Math.max(trackingToken.getToken(), 0) - 1);
    }

    @Override
    public CompletableFuture<Long> getTokenAt(long instant) {
        FutureStreamObserver<TrackingToken> result = new FutureStreamObserver<>(NO_TOKEN_AVAILABLE);
        eventStore.getTokenAt(GetTokenAtRequest.newBuilder().setInstant(instant).build(), result);
        return result.thenApply(TrackingToken::getToken);
    }

    private AggregateEventStream doGetAggregateStream(GetAggregateEventsRequest request) {
        BufferedAggregateEventStream buffer = new BufferedAggregateEventStream();
        eventStore.listAggregateEvents(request, buffer);
        return buffer;
    }
}
