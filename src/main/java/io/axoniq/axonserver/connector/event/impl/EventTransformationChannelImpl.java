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
import io.axoniq.axonserver.grpc.event.StartTransformationRequest;
import io.axoniq.axonserver.grpc.event.TransformRequest;
import io.axoniq.axonserver.grpc.event.TransformRequestAck;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public class EventTransformationChannelImpl extends AbstractAxonServerChannel<Void>
        implements EventTransformationChannel {

    private final EventTransformationServiceGrpc.EventTransformationServiceStub eventTransformationService;

    private final ClientIdentification clientIdentification;

    /**
     * todo
     */
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
        return responseObserver.thenApply(this::startedTransformationStep);
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
            private final ConcurrentHashMap<Long, CompletableFuture<Void>> transformationsInProgress = new ConcurrentHashMap<>();
            private final AtomicBoolean isFailed = new AtomicBoolean(false);
            private final AtomicReference<Throwable> failure = new AtomicReference<>();
            private final AtomicLong sequenceNumber = new AtomicLong(-1);
            private StreamObserver<TransformRequest> transformEventsRequestStreamObserver;

            private EventTransformation initializeState() {
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
                        isFailed.set(true);
                        failure.set(t);
                        transformationsInProgress.values().forEach(c -> c.completeExceptionally(t));
                    }
                    //todo or its stream is closed
                    TransformRequestAck ack = results.nextIfAvailable();
                    if (ack != null) {
                        CompletableFuture<Void> transformedFuture = transformationsInProgress.remove(ack.getSequence());
                        transformedFuture.complete(null);
                    }
                });

                transformEventsRequestStreamObserver = eventTransformationService.transformEvents(
                        results);

                return this;
            }

            private CompletableFuture<Void> transformEvent(TransformRequest request) {
                CompletableFuture<Void> future = transformationsInProgress.computeIfAbsent(request.getSequence(),
                                                                                           k -> new CompletableFuture<>());
                transformEventsRequestStreamObserver.onNext(request);

                return future;
            }

            private CompletableFuture<Void> cancelTransformation(
                    io.axoniq.axonserver.connector.event.EventTransformation.TransformationId request) {
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
                    io.axoniq.axonserver.connector.event.EventTransformation.TransformationId request) {
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

            private ApplyOrCancelEventTransformation applyOrCancelEventTransformationStep(Void confirmation) {
                EventTransformation parent = this;
                return new ApplyOrCancelEventTransformation() {

                    @Override
                    public TransformationId id() {
                        return parent.id();
                    }

                    @Override
                    public CompletableFuture<ApplyOrCancelEventTransformation> replaceEvent(long token, Event event) {
                        return parent.replaceEvent(token, event);
                    }

                    @Override
                    public CompletableFuture<ApplyOrCancelEventTransformation> deleteEvent(long token) {
                        return parent.deleteEvent(token);
                    }

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
                                                                             //.setLastEventToken(lastEventToken.get()) todo replace with sequence number?
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
            public CompletableFuture<ApplyOrCancelEventTransformation> replaceEvent(long token,
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
                                                      .build()).thenApply(this::applyOrCancelEventTransformationStep);
            }

            @Override
            public CompletableFuture<ApplyOrCancelEventTransformation> deleteEvent(long token) {
                return transformEvent(TransformRequest.newBuilder()
                                                      .setTransformationId(io.axoniq.axonserver.grpc.event.TransformationId.newBuilder()
                                                                                                                           .setId(id().id())
                                                                                                                           .build())
                                                      .setSequence(sequenceNumber.incrementAndGet())
                                                      .setDeleteEvent(DeletedEvent.newBuilder()
                                                                                  .setToken(token)
                                                                                  .build())
                                                      .build())
                        .thenApply(this::applyOrCancelEventTransformationStep);
            }
        }.initializeState();
    }
}
