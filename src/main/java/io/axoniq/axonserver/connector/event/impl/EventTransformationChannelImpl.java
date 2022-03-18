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
        return responseObserver.thenApply(this::startedTransformationStep);
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


    private CompletableFuture<Confirmation> cancelTransformation(
            io.axoniq.axonserver.connector.event.EventTransformation.TransformationId request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.cancelTransformation(TransformationId.newBuilder().setId(request.id()).build(),
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

    private CompletableFuture<Confirmation> rollbackTransformation(
            io.axoniq.axonserver.connector.event.EventTransformation.TransformationId request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while executing rollback on transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.rollbackTransformation(TransformationId.newBuilder().setId(request.id()).build(),
                                                          responseObserver);
        return responseObserver;
    }

    private CompletableFuture<Confirmation> deleteOldVersions(
            io.axoniq.axonserver.connector.event.EventTransformation.TransformationId request) {
        FutureStreamObserver<Confirmation> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.deleteOldVersions(TransformationId.newBuilder().setId(request.id()).build(),
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

    private EventTransformation startedTransformationStep(TransformationId transformationId) {
        return new EventTransformation() {

            private final AtomicLong lastEventToken = new AtomicLong(-1);

            private ApplyOrCancelEventTransformation applyOrCancelEventTransformationStep(Void confirmation) {
                return new ApplyOrCancelEventTransformation() {

                    private CompletableFuture<Void> rollbackEventTransformationStep() {
                        return rollbackTransformation(
                                id()).thenAccept(n -> {
                        });
                    }

                    @Override
                    public CompletableFuture<RollbackEventTransformation> apply() {
                        return apply(true);
                    }

                    @Override
                    public CompletableFuture<RollbackEventTransformation> apply(
                            boolean keepBackup) {
                        return applyTransformation(ApplyTransformationRequest.newBuilder()
                                                                             .setTransformationId(io.axoniq.axonserver.grpc.event.TransformationId.newBuilder()
                                                                                                                                                  .setId(id().id())
                                                                                                                                                  .build())
                                                                             .setLastEventToken(lastEventToken.get())
                                                                             .setKeepOldVersions(keepBackup)
                                                                             .build()).thenApply(confirmation -> this::rollbackEventTransformationStep);
                    }

                    @Override
                    public CompletableFuture<Void> cancel() {
                        return cancelTransformation(id()).thenAccept(n -> {});
                    }
                };
            }

            @Override
            public TransformationId id() {
                return transformationId::getId;
            }

            @Override
            public CompletableFuture<ApplyOrCancelEventTransformation> replaceEvent(long token, long previousToken,
                                                                                    Event event) {
                return transformEvent(TransformEventsRequest.newBuilder()
                                                            .setEvent(TransformedEvent.newBuilder()
                                                                                      .setEvent(event)
                                                                                      .setToken(token)
                                                                                      .setPreviousToken(
                                                                                              previousToken)
                                                                                      .build())
                                                            .build()).thenRun(() -> lastEventToken.set(token))
                                                                     .thenApply(this::applyOrCancelEventTransformationStep);
            }

            @Override
            public CompletableFuture<ApplyOrCancelEventTransformation> deleteEvent(long token, long previousToken) {
                return transformEvent(TransformEventsRequest.newBuilder()
                                                            .setDeleteEvent(DeletedEvent.newBuilder()
                                                                                        .setToken(token)
                                                                                        .setPreviousToken(
                                                                                                previousToken)
                                                                                        .build())
                                                            .build())
                        .thenRun(() -> lastEventToken.set(token))
                        .thenApply(this::applyOrCancelEventTransformationStep);
            }

            @Override
            public CompletableFuture<Void> cleanUpBackupFiles() {
                return deleteOldVersions(id()).thenAccept(confirmation -> {
                });
            }
        };
    }
}
