package io.axoniq.axonserver.connector.event.impl;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.event.EventTransformation;
import io.axoniq.axonserver.connector.event.EventTransformationChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.FutureListStreamObserver;
import io.axoniq.axonserver.connector.impl.FutureStreamObserver;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.event.ApplyTransformationRequest;
import io.axoniq.axonserver.grpc.event.DeletedEvent;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventTransformationServiceGrpc;
import io.axoniq.axonserver.grpc.event.StartTransformationRequest;
import io.axoniq.axonserver.grpc.event.TransformRequest;
import io.axoniq.axonserver.grpc.event.TransformRequestAck;
import io.axoniq.axonserver.grpc.event.Transformation;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public class EventTransformationChannelImpl extends AbstractAxonServerChannel<Void>
        implements EventTransformationChannel {

    private final EventTransformationServiceGrpc.EventTransformationServiceStub eventTransformationService;

    private final ClientIdentification clientIdentification;

    public EventTransformationChannelImpl(ClientIdentification clientIdentification, ScheduledExecutorService executor,
                                          AxonServerManagedChannel channel) {
        super(clientIdentification, executor, channel);
        this.clientIdentification = clientIdentification;
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
        return responseObserver.thenApply(ActiveEventTransformation::new);
    }

    @Override
    public CompletableFuture<List<EventTransformation>> transformations() {
        FutureListStreamObserver<Transformation> responseObserver = new FutureListStreamObserver<>();
        eventTransformationService.transformations(Empty.newBuilder().build(), responseObserver);
        return responseObserver.thenApply(n -> n.stream().map(ActiveEventTransformation::new)
                                                .collect(Collectors.toList()));
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

    class ActiveEventTransformation implements EventTransformation {

        private final TransformationId transformationId;
        private final ConcurrentHashMap<Long, CompletableFuture<Void>> transformationsInProgress = new ConcurrentHashMap<>();
        private final AtomicReference<TransformationState> transformationState = new AtomicReference<>();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicLong sequenceNumber = new AtomicLong(-1);
        private StreamObserver<TransformRequest> transformEventsRequestStreamObserver;


        ActiveEventTransformation(io.axoniq.axonserver.grpc.event.TransformationId transformationId) {
            this.transformationId = transformationId::getId;
            transformationState.set(TransformationState.ACTIVE);
            initializeConnection();
        }

        ActiveEventTransformation(Transformation transformation) {
            this.transformationId = () -> transformation.getTransformationId().getId();
            sequenceNumber.set(transformation.getSequence());

            if (transformation.getError().isInitialized()) {
                transformationState.set(TransformationState.FATAL);
                failure.set(new AxonServerException(transformation.getError()));
            } else {
                transformationState.set(TransformationState.valueOf(transformation.getState().getValueDescriptor()
                                                                                  .getName()));
                initializeConnection();
            }
        }

        private void initializeConnection() {
            AbstractBufferedStream<TransformRequestAck, TransformRequest> results = new AbstractBufferedStream<TransformRequestAck, TransformRequest>(
                    clientIdentification.getClientId(), 32, 8
            ) {
                @Override
                protected TransformRequest buildFlowControlMessage(FlowControl flowControl) {
                    return null;
                }

                @Override
                protected TransformRequestAck terminalMessage() {
                    return null;
                }
            };

            results.onAvailable(() -> {
                if (results.getError().isPresent()) {
                    Throwable t = results.getError().get();
                    transformationState.set(TransformationState.FATAL);
                    failure.set(t);
                    transformationsInProgress.values().forEach(c -> c.completeExceptionally(t));
                } else if (results.isClosed()) {
                    transformationState.set(TransformationState.FATAL);
                    failure.set(new AxonServerException(ErrorCategory.CONNECTION_FAILED,
                                                        "Connection was cancelled",
                                                        ""));
                } else {
                    TransformRequestAck ack = results.nextIfAvailable();
                    if (ack != null) {
                        CompletableFuture<Void> transformedFuture = transformationsInProgress.remove(ack.getSequence());
                        transformedFuture.complete(null);
                    }
                }
            });

            transformEventsRequestStreamObserver = eventTransformationService.transformEvents(
                    results);
        }

        private CompletableFuture<Void> transformEvent(TransformRequest request) {
            if (transformationState.get() == TransformationState.FATAL) {
                throw new RuntimeException(failure.get());
            }

            CompletableFuture<Void> future = transformationsInProgress.computeIfAbsent(request.getSequence(),
                                                                                       k -> new CompletableFuture<>());

            try {
                transformEventsRequestStreamObserver.onNext(request);
            } catch (Throwable t) {
                transformationState.set(TransformationState.FATAL);
                failure.set(t);
                transformationsInProgress.remove(request.getSequence());
                throw new RuntimeException(t);
            }

            return future;
        }

        private CompletableFuture<Void> cancelTransformation(
                EventTransformation.TransformationId request) {
            FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                    ErrorCategory.OTHER,
                    "An unknown error occurred while cancelling transformation. No response received from Server.",
                    ""
            ));

            eventTransformationService.cancelTransformation(io.axoniq.axonserver.grpc.event.TransformationId.newBuilder()
                                                                                                            .setId(request.id())
                                                                                                            .build(),
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
                EventTransformation.TransformationId request) {
            FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                    ErrorCategory.OTHER,
                    "An unknown error occurred while executing rollback on transformation. No response received from Server.",
                    ""
            ));

            eventTransformationService.rollbackTransformation(io.axoniq.axonserver.grpc.event.TransformationId.newBuilder()
                                                                                                              .setId(request.id())
                                                                                                              .build(),
                                                              responseObserver);
            return responseObserver.thenAccept(empty -> {
            });
        }

        @Override
        public CompletableFuture<Void> rollback() {
            if (transformationState.get() == TransformationState.FATAL) {
                throw new RuntimeException(failure.get());
            }
            transformationState.set(TransformationState.ROLLING_BACK);
            return rollbackTransformation(
                    transformationId).thenAccept(n -> transformationState.set(TransformationState.ROLLED_BACK));
        }

        @Override
        public CompletableFuture<EventTransformation> apply() {
            return apply(true);
        }

        @Override
        public CompletableFuture<EventTransformation> apply(
                boolean keepBackup) {
            if (transformationState.get() == TransformationState.FATAL) {
                throw new RuntimeException(failure.get());
            }
            transformationState.set(TransformationState.APPLYING);
            return applyTransformation(ApplyTransformationRequest.newBuilder()
                                                                 .setTransformationId(io.axoniq.axonserver.grpc.event.TransformationId.newBuilder()
                                                                                                                                      .setId(id().id())
                                                                                                                                      .build())
                                                                 .setLastSequence(sequenceNumber.get())
                                                                 .setKeepOldVersions(keepBackup)
                                                                 .build())
                    .thenApply(confirmation -> {
                        transformationState.set(TransformationState.APPLIED);
                        return this;
                    });
        }

        @Override
        public CompletableFuture<Void> cancel() {
            if (transformationState.get() == TransformationState.FATAL) {
                throw new RuntimeException(failure.get());
            }
            transformationState.set(TransformationState.CANCELLING);
            return cancelTransformation(id()).thenAccept(n -> transformationState.set(TransformationState.CANCELLED));
        }

        @Override
        public TransformationId id() {
            return transformationId;
        }

        @Override
        public CompletableFuture<EventTransformation> replaceEvent(long token,
                                                                   Event event) {
            return transformEvent(TransformRequest.newBuilder()
                                                  .setTransformationId(io.axoniq.axonserver.grpc.event.TransformationId.newBuilder()
                                                                                                                       .setId(id().id())
                                                                                                                       .build())
                                                  .setSequence(sequenceNumber.incrementAndGet())
                                                  .setEvent(TransformedEvent.newBuilder()
                                                                            .setEvent(event)
                                                                            .setToken(token)

                                                                            .build())
                                                  .build()).thenApply(confirmation -> this);
        }

        @Override
        public CompletableFuture<EventTransformation> deleteEvent(long token) {
            return transformEvent(TransformRequest.newBuilder()
                                                  .setTransformationId(io.axoniq.axonserver.grpc.event.TransformationId.newBuilder()
                                                                                                                       .setId(id().id())
                                                                                                                       .build())
                                                  .setSequence(sequenceNumber.incrementAndGet())
                                                  .setDeleteEvent(DeletedEvent.newBuilder()
                                                                              .setToken(token)
                                                                              .build())
                                                  .build())
                    .thenApply(confirmation -> this);
        }
    }
}

