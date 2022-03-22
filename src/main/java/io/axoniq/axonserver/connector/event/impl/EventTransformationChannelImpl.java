package io.axoniq.axonserver.connector.event.impl;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.event.EventTransformation;
import io.axoniq.axonserver.connector.event.EventTransformationChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.FutureStreamObserver;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.event.ApplyTransformationRequest;
import io.axoniq.axonserver.grpc.event.DeletedEvent;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventTransformationServiceGrpc;
import io.axoniq.axonserver.grpc.event.EventTransformedAck;
import io.axoniq.axonserver.grpc.event.StartTransformationRequest;
import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public class EventTransformationChannelImpl extends AbstractAxonServerChannel<Void>
        implements EventTransformationChannel {

    private final EventTransformationServiceGrpc.EventTransformationServiceStub eventTransformationService;
    private final ConcurrentHashMap<Long, CompletableFuture<Void>> transformationsInProgress = new ConcurrentHashMap<>();
    private StreamObserver<TransformEventsRequest> transformEventsRequestStreamObserver;
    private AtomicBoolean isFailed = new AtomicBoolean(false);

    /**
     * todo
     */
    public EventTransformationChannelImpl(ClientIdentification clientIdentification, ScheduledExecutorService executor,
                                          AxonServerManagedChannel channel) {
        super(clientIdentification, executor, channel);
        eventTransformationService = EventTransformationServiceGrpc.newStub(channel);
    }

    @Override
    public CompletableFuture<EventTransformation> newTransformation(String description) {
        FutureStreamObserver<TransformationId> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while starting transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.startTransformation(StartTransformationRequest.newBuilder()
                                                                                 .setDescription(description).build(),
                                                       responseObserver);
        initTransformChannel();
        return responseObserver.thenApply(this::startedTransformationStep); //todo point to self replace event and remove event
    }

    private void initTransformChannel() {
        AbstractBufferedStream<EventTransformedAck, TransformEventsRequest> results = new AbstractBufferedStream<EventTransformedAck, TransformEventsRequest>(
                "??? todo", 32, 8
        ) {

            @Override
            public void onError(Throwable t) {
                isFailed.set(true);
                transformationsInProgress.values().forEach(c->c.completeExceptionally(t));
                super.onError(t);
            }

            @Override
            protected TransformEventsRequest buildFlowControlMessage(FlowControl flowControl) {
                return null;
            }

            @Override
            protected EventTransformedAck terminalMessage() {
                return null;
            }
        };

        results.onAvailable(() -> {
            EventTransformedAck ack = results.nextIfAvailable();
            if (ack != null) {
                CompletableFuture<Void> transformedFuture = transformationsInProgress.remove(ack.getToken());
                transformedFuture.complete(null);
            }
        });

        transformEventsRequestStreamObserver = eventTransformationService.transformEvents(
                results);
    }


    private CompletableFuture<Void> transformEvent(TransformEventsRequest request) {
        long token = -1;
        if (request.hasEvent()) {
            token = request.getEvent().getToken();
        } else if (request.hasDeleteEvent()) {
            token = request.getDeleteEvent().getToken();
        }
        CompletableFuture<Void> future = transformationsInProgress.computeIfAbsent(token,
                                                                                   k -> new CompletableFuture<>());
        transformEventsRequestStreamObserver.onNext(request); //todo: should be synchronized?

        return future;
    }


    private CompletableFuture<Void> cancelTransformation(
            io.axoniq.axonserver.connector.event.EventTransformation.TransformationId request) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.cancelTransformation(TransformationId.newBuilder().setId(request.id()).build(),
                                                        responseObserver);
        return responseObserver.thenAccept(empty -> completeTransformationChannel());
    }

    private CompletableFuture<Void> applyTransformation(ApplyTransformationRequest request) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while applying transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.applyTransformation(request,
                                                       responseObserver);
        return responseObserver.thenAccept(empty -> completeTransformationChannel());
    }

    private void completeTransformationChannel() {
        transformEventsRequestStreamObserver.onCompleted();
    }

    private CompletableFuture<Void> rollbackTransformation(
            io.axoniq.axonserver.connector.event.EventTransformation.TransformationId request) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while executing rollback on transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.rollbackTransformation(TransformationId.newBuilder().setId(request.id()).build(),
                                                          responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Override
    public CompletableFuture<Void> deleteOldVersions() {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while cancelling transformation. No response received from Server.",
                ""
        ));

        eventTransformationService.deleteOldVersions(Empty.newBuilder().build(), responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
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
                        return cancelTransformation(id()).thenAccept(n -> {
                        });
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
                                                            .setTransformationId(io.axoniq.axonserver.grpc.event.TransformationId.newBuilder()
                                                                                                                                 .setId(id().id())
                                                                                                                                 .build())
                                                            .setPreviousToken(
                                                                    previousToken)
                                                            .setEvent(TransformedEvent.newBuilder()
                                                                                      .setEvent(event)
                                                                                      .setToken(token)

                                                                                      .build())
                                                            .build()).thenRun(() -> lastEventToken.set(token))
                                                                     .thenApply(this::applyOrCancelEventTransformationStep);
            }

            @Override
            public CompletableFuture<ApplyOrCancelEventTransformation> deleteEvent(long token, long previousToken) {
                return transformEvent(TransformEventsRequest.newBuilder()
                                                            .setTransformationId(io.axoniq.axonserver.grpc.event.TransformationId.newBuilder()
                                                                                                                                 .setId(id().id())
                                                                                                                                 .build())
                                                            .setPreviousToken(
                                                                    previousToken)
                                                            .setDeleteEvent(DeletedEvent.newBuilder()
                                                                                        .setToken(token)
                                                                                        .build())
                                                            .build())
                        .thenRun(() -> lastEventToken.set(token))
                        .thenApply(this::applyOrCancelEventTransformationStep);
            }
        };
    }
}
