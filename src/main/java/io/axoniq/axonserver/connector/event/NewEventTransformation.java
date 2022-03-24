package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public interface NewEventTransformation {

    TransformationId id();
    CompletableFuture<ActiveEventTransformation> replaceEvent(long token, Event event);
    CompletableFuture<ActiveEventTransformation> deleteEvent(long token);

    interface ActiveEventTransformation extends NewEventTransformation {
        CompletableFuture<ActiveEventTransformation> apply();
        CompletableFuture<ActiveEventTransformation> apply(boolean keepBackup);
        CompletableFuture<Void> cancel();
        CompletableFuture<Void> rollback();
    }

    interface TransformationId {
        String id();
    }

}

