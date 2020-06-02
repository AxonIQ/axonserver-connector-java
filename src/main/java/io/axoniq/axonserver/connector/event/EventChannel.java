package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.concurrent.CompletableFuture;

public interface EventChannel {
    AppendEventsTransaction startAppendEventsTransaction();

    CompletableFuture<Long> findHighestSequence(String aggregateId);

    default ResultStream<EventWithToken> openStream(long token, int bufferSize) {
        return openStream(token, bufferSize, Math.max(bufferSize >> 3, 100));
    }

    ResultStream<EventWithToken> openStream(long token, int bufferSize, int refillBatch);

    default AggregateEventStream openAggregateStream(String aggregateIdentifier) {
        return openAggregateStream(aggregateIdentifier, true);
    }

    AggregateEventStream openAggregateStream(String aggregateIdentifier, boolean allowSnapshots);

    AggregateEventStream openAggregateStream(String aggregateIdentifier, long initialSequence);

    CompletableFuture<Confirmation> appendSnapshot(Event snapshotEvent);

    AggregateEventStream loadSnapshots(String aggregateIdentifier, long initialSequence, long maxSequence, int maxResults);

    default AggregateEventStream loadSnapshots(String aggregateIdentifier, long maxSequence, int maxResults) {
        return loadSnapshots(aggregateIdentifier, 0, maxSequence, maxResults);
    }

    default AggregateEventStream loadSnapshot(String aggregateIdentifier) {
        return loadSnapshots(aggregateIdentifier, 0, Long.MAX_VALUE, 1);
    }

    CompletableFuture<Long> getLastToken();

    CompletableFuture<Long> getFirstToken();

    CompletableFuture<Long> getTokenAt(long instant);
}
