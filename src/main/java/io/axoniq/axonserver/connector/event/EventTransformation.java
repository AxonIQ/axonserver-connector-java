package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface EventTransformation {

    TransformationId id();

    CompletableFuture<ApplyOrCancelEventTransformation> replaceEvent(long token, long previousToken, Event event);

    CompletableFuture<ApplyOrCancelEventTransformation> deleteEvent(long token, long previousToken);

    CompletableFuture<Void> cleanUpBackupFiles();

    interface ApplyOrCancelEventTransformation {
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

