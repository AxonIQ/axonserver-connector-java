package io.axoniq.axonserver.connector.event.impl;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.event.EventTransformation;
import io.axoniq.axonserver.connector.event.EventTransformationChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.FutureStreamObserver;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.event.ApplyTransformationRequest;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.DeletedEvent;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventTransformationServiceGrpc;
import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public class EventTransformationChannelImpl extends AbstractAxonServerChannel<Void>
        implements EventTransformationChannel {

    private final EventTransformationServiceGrpc.EventTransformationServiceStub eventTransformationService;

    /**
     * todo
     */
    public EventTransformationChannelImpl(ClientIdentification clientIdentification, ScheduledExecutorService executor,
                                          AxonServerManagedChannel channel) {
        super(clientIdentification, executor, channel);
        eventTransformationService = EventTransformationServiceGrpc.newStub(channel);
    }

    @Override
    public CompletableFuture<EventTransformation> newTransformation() {
        FutureStreamObserver<TransformationId> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while starting transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.startTransformation(Empty.newBuilder().build(),
                                                       responseObserver);
        return responseObserver.thenApply(transformationId -> new EventTransformation() {

            private final AtomicLong lastEventToken = new AtomicLong(-1);

            @Override
            public TransformationId transformationId() {
                return transformationId;
            }

            @Override
            public CompletableFuture<EventTransformation> replaceEvent(long token, long previousToken, Event event) {
                return transformEvent(TransformEventsRequest.newBuilder()
                                                            .setEvent(TransformedEvent.newBuilder()
                                                                                      .setEvent(event)
                                                                                      .setToken(token)
                                                                                      .setPreviousToken(previousToken)
                                                                                      .build())
                                                            .build()).thenRun(() -> lastEventToken.set(token))
                                                                     .thenApply(confirmation -> this);
            }

            @Override
            public CompletableFuture<EventTransformation> deleteEvent(long token, long previousToken) {
                return transformEvent(TransformEventsRequest.newBuilder()
                                                            .setDeleteEvent(DeletedEvent.newBuilder()
                                                                                        .setToken(token)
                                                                                        .setPreviousToken(previousToken)
                                                                                        .build())
                                                            .build())
                        .thenRun(() -> lastEventToken.set(token))
                        .thenApply(confirmation -> this);
            }

            @Override
            public CompletableFuture<EventTransformation> apply() {
                return applyTransformation(ApplyTransformationRequest.newBuilder()
                                                                     .setTransformationId(transformationId())
                                                                     .setLastEventToken(lastEventToken.get())
                                                                     .setKeepOldVersions(true)
                                                                     .build()).thenApply(confirmation -> this);
            }

            @Override
            public CompletableFuture<EventTransformation> cancel() {
                return cancelTransformation(transformationId()).thenApply(confirmation -> this);
            }

            @Override
            public CompletableFuture<EventTransformation> rollback() {
                return rollbackTransformation(transformationId()).thenApply(confirmation -> this);
            }

            @Override
            public CompletableFuture<EventTransformation> cleanUp() {
                return deleteOldVersions(transformationId()).thenApply(confirmation -> this);
            }
        });
    }


    private CompletableFuture<Confirmation> transformEvents(List<TransformEventsRequest> request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        StreamObserver<TransformEventsRequest> transformEventsRequestStreamObserver = eventTransformationService.transformEvents(
                responseObserver);
        request.forEach(transformEventsRequestStreamObserver::onNext);
        //   transformEventsRequestStreamObserver.onCompleted();

        return responseObserver;
    }


    private CompletableFuture<Confirmation> transformEvent(TransformEventsRequest request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        StreamObserver<TransformEventsRequest> transformEventsRequestStreamObserver = eventTransformationService.transformEvents(
                responseObserver);
        transformEventsRequestStreamObserver.onNext(request);
        //  transformEventsRequestStreamObserver.onCompleted();

        return responseObserver;
    }


    private CompletableFuture<Confirmation> cancelTransformation(TransformationId request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.cancelTransformation(request,
                                                        responseObserver);
        return responseObserver;
    }

    private CompletableFuture<Confirmation> applyTransformation(ApplyTransformationRequest request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while applying transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.applyTransformation(request,
                                                       responseObserver);
        return responseObserver;
    }

    private CompletableFuture<Confirmation> rollbackTransformation(TransformationId request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while executing rollback on transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.rollbackTransformation(request,
                                                          responseObserver);
        return responseObserver;
    }

    private CompletableFuture<Confirmation> deleteOldVersions(TransformationId request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.deleteOldVersions(request,
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
