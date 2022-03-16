package io.axoniq.axonserver.connector.event.impl;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.event.EventTransformationChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.FutureStreamObserver;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.event.ApplyTransformationRequest;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.EventTransformationServiceGrpc;
import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Stefan Dragisic
 */
public class EventTransformationImpl extends AbstractAxonServerChannel<Void> implements EventTransformationChannel {

    private final EventTransformationServiceGrpc.EventTransformationServiceStub eventTransformationService;

    /**
     * todo
     */
    public EventTransformationImpl(ClientIdentification clientIdentification, ScheduledExecutorService executor,
                                   AxonServerManagedChannel channel) {
        super(clientIdentification, executor, channel);
        eventTransformationService = EventTransformationServiceGrpc.newStub(channel);
    }

    @Override
    public CompletableFuture<TransformationId> startTransformation() {
        FutureStreamObserver<TransformationId> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while starting transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.startTransformation(Empty.newBuilder().build(),
                                                       responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<Confirmation> transformEvents(List<TransformEventsRequest> request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        StreamObserver<TransformEventsRequest> transformEventsRequestStreamObserver = eventTransformationService.transformEvents(
                responseObserver);
        request.forEach(transformEventsRequestStreamObserver::onNext);

        return responseObserver;
    }

    @Override
    public CompletableFuture<Confirmation> cancelTransformation(TransformationId request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.cancelTransformation(request,
                                                        responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<Confirmation> applyTransformation(ApplyTransformationRequest request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.applyTransformation(request,
                                                       responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<Confirmation> rollbackTransformation(TransformationId request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.rollbackTransformation(request,
                                                          responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<Confirmation> deleteOldVersions(TransformationId request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.rollbackTransformation(request,
                                                          responseObserver);
        return responseObserver;
    }

    @Override
    public synchronized void connect() {
        // there is no instruction stream for the events channel
    }

    @Override
    public void reconnect() {

    }

    @Override
    public void disconnect() {

    }

    @Override
    public boolean isReady() {
        return true;
    }
}
