package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public interface EventTransformation {
    TransformationId id();
    CompletableFuture<EventTransformation> replaceEvent(long token, Event event);
    CompletableFuture<EventTransformation> deleteEvent(long token);
    CompletableFuture<EventTransformation> apply();
    CompletableFuture<EventTransformation> apply(boolean keepBackup);
    CompletableFuture<Void> cancel();
    CompletableFuture<Void> rollback();

    interface TransformationId {
        String id();
    }
}