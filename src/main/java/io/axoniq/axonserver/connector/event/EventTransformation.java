package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.TransformationId;

import java.util.concurrent.CompletableFuture;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface EventTransformation {

    TransformationId transformationId();

    CompletableFuture<EventTransformation> replaceEvent(long token, long previousToken, Event event);

    CompletableFuture<EventTransformation> deleteEvent(long token, long previousToken);

    CompletableFuture<EventTransformation> apply();

    CompletableFuture<EventTransformation> cancel();

    CompletableFuture<EventTransformation> cleanUp();

    CompletableFuture<EventTransformation> rollback();

}
