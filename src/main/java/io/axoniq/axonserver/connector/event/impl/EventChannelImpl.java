/*
 * Copyright (c) 2010-2020. Axon Framework
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

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.event.AggregateEventStream;
import io.axoniq.axonserver.connector.event.AppendEventsTransaction;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.FutureStreamObserver;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventStoreGrpc;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetFirstTokenRequest;
import io.axoniq.axonserver.grpc.event.GetLastTokenRequest;
import io.axoniq.axonserver.grpc.event.GetTokenAtRequest;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrRequest;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrResponse;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class EventChannelImpl extends AbstractAxonServerChannel implements EventChannel {

    private static final Logger logger = LoggerFactory.getLogger(EventChannel.class);
    public static final ReadHighestSequenceNrResponse UNKNOWN_HIGHEST_SEQ = ReadHighestSequenceNrResponse.newBuilder().setToSequenceNr(-1).build();
    public static final TrackingToken NO_TOKEN_AVAILABLE = TrackingToken.newBuilder().setToken(-1).build();
    private final EventStoreGrpc.EventStoreStub eventStore;
    // guarded by -this-

    public EventChannelImpl(ScheduledExecutorService executor, ManagedChannel channel) {
        super(executor, channel);
        eventStore = EventStoreGrpc.newStub(channel);
    }

    @Override
    public synchronized void connect(ManagedChannel channel) {
        // there is no instruction stream for the events channel
    }

    @Override
    public void disconnect() {
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
    public CompletableFuture<Long> findHighestSequence(String aggregateId) {
        FutureStreamObserver<ReadHighestSequenceNrResponse> result = new FutureStreamObserver<>(UNKNOWN_HIGHEST_SEQ);
        eventStore.readHighestSequenceNr(ReadHighestSequenceNrRequest.newBuilder()
                                                                           .setAggregateId(aggregateId)
                                                                           .build(),
                                               result);
        return result.thenApply(ReadHighestSequenceNrResponse::getToSequenceNr);
    }

    @Override
    public ResultStream<EventWithToken> openStream(long token, int bufferSize, int refillBatch) {
        BufferedEventStream buffer = new BufferedEventStream(Math.max(64, bufferSize), Math.max(16, Math.min(bufferSize, refillBatch)));
        //noinspection ResultOfMethodCallIgnored
        eventStore.listEvents(buffer);
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
    public AggregateEventStream openAggregateStream(String aggregateIdentifier, long initialSequence) {
        return doGetAggregateStream(GetAggregateEventsRequest.newBuilder()
                                                             .setAggregateId(aggregateIdentifier)
                                                             .setInitialSequence(initialSequence)
                                                             .build());
    }

    @Override
    public CompletableFuture<?> appendSnapshot(Event snapshotEvent) {
        FutureStreamObserver<Confirmation> result = new FutureStreamObserver<>(Confirmation.newBuilder().setSuccess(false).build());
        eventStore.appendSnapshot(snapshotEvent, result);
        return result;
    }

    @Override
    public AggregateEventStream loadSnapshots(String aggregateIdentifier, long initialSequence, long maxSequence, int maxResults) {
        BufferedAggregateEventStream buffer = new BufferedAggregateEventStream(maxResults);
        eventStore.listAggregateSnapshots(GetAggregateSnapshotsRequest.newBuilder()
                                                                            .setInitialSequence(initialSequence)
                                                                            .setMaxResults(maxResults)
                                                                            .setMaxSequence(maxSequence)
                                                                            .setAggregateId(aggregateIdentifier)
                                                                            .build(), buffer);
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
        return result.thenApply(TrackingToken::getToken);
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
