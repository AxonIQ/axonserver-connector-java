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
 */
public interface EventTransformationChannel {

    CompletableFuture<TransformationId> startTransformation();
    CompletableFuture<Confirmation> transformEvents(List<TransformEventsRequest> request);
    CompletableFuture<Confirmation> cancelTransformation(TransformationId request);
    CompletableFuture<Confirmation> applyTransformation(ApplyTransformationRequest request);
    CompletableFuture<Confirmation> rollbackTransformation(TransformationId request);
    CompletableFuture<Confirmation> deleteOldVersions(TransformationId request);

}