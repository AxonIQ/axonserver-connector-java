package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public interface EventTransformation {
    TransformationId id();
    TransformationState state();
    CompletableFuture<EventTransformation> replaceEvent(long token, Event event);
    CompletableFuture<EventTransformation> deleteEvent(long token);
    CompletableFuture<EventTransformation> apply();
    CompletableFuture<Void> cancel();
    CompletableFuture<Void> rollback();

    interface TransformationId {
        String id();
    }

    enum TransformationState {
        ACTIVE,
        ROLLING_BACK,
        ROLLED_BACK,
        CANCELLING,
        CANCELLED,
        APPLYING,
        APPLIED,
        FATAL,
        UNKNOWN
    }
}