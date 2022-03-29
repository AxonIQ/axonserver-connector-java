package io.axoniq.axonserver.connector.event;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public interface EventTransformationChannel {

    CompletableFuture<List<EventTransformation>> transformations();
    CompletableFuture<EventTransformation> newTransformation(String description);
    CompletableFuture<Void> deleteOldVersions();
}