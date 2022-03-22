package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public interface EventTransformation {

    TransformationId id();
    CompletableFuture<ApplyOrCancelEventTransformation> replaceEvent(long token, Event event);
    CompletableFuture<ApplyOrCancelEventTransformation> deleteEvent(long token);

    interface ApplyOrCancelEventTransformation extends EventTransformation {
        CompletableFuture<RollbackEventTransformation> apply();
        CompletableFuture<RollbackEventTransformation> apply(boolean keepBackup);
        CompletableFuture<Void> cancel();
    }

    interface RollbackEventTransformation {
        CompletableFuture<Void> rollback();
    }

    interface TransformationId {
        String id();
    }

}

