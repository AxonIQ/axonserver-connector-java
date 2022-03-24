package io.axoniq.axonserver.connector.event;

import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public interface EventTransformationChannel {

    //todo get all transformation
    //todo get transformation by id
    CompletableFuture<NewEventTransformation> newTransformation(String description);
    CompletableFuture<Void> deleteOldVersions();
}