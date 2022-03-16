package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.event.ApplyTransformationRequest;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import io.axoniq.axonserver.grpc.event.TransformationId;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public interface EventTransformationChannel {

    //todo get all transformation
    //todo get transformation by id
    CompletableFuture<EventTransformation> newTransformation();

}