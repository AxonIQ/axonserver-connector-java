/*
 * Copyright (c) 2020-2023. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.event.transformation.impl.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.event.transformation.EventTransformation;
import io.axoniq.axonserver.connector.event.transformation.impl.EventTransformationService;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.FutureListStreamObserver;
import io.axoniq.axonserver.connector.impl.FutureStreamObserver;
import io.axoniq.axonserver.grpc.event.ApplyTransformationRequest;
import io.axoniq.axonserver.grpc.event.CompactionRequest;
import io.axoniq.axonserver.grpc.event.EventTransformationServiceGrpc;
import io.axoniq.axonserver.grpc.event.StartTransformationRequest;
import io.axoniq.axonserver.grpc.event.TransformRequestAck;
import io.axoniq.axonserver.grpc.event.Transformation;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class GrpcEventTransformationService implements EventTransformationService {

    private final Logger logger = LoggerFactory.getLogger(GrpcEventTransformationService.class);

    private final EventTransformationServiceGrpc.EventTransformationServiceStub stub;

    public GrpcEventTransformationService(AxonServerManagedChannel axonServerManagedChannel) {
        this(EventTransformationServiceGrpc.newStub(axonServerManagedChannel));
    }

    public GrpcEventTransformationService(EventTransformationServiceGrpc.EventTransformationServiceStub stub) {
        this.stub = stub;
    }

    @Nonnull
    private static ApplyTransformationRequest applyTransformationRequest(String transformationId, Long sequence) {
        return ApplyTransformationRequest
                .newBuilder()
                .setTransformationId(TransformationId.newBuilder().setId(transformationId))
                .setLastSequence(sequence)
                .build();
    }

    @Override
    public CompletableFuture<Iterable<EventTransformation>> transformations() {
        FutureListStreamObserver<Transformation> responseObserver = new FutureListStreamObserver<>();
        stub.transformations(Empty.newBuilder().build(), responseObserver);
        return responseObserver.thenApply(n -> n.stream()
                                                .map(GrpcEventTransformation::new)
                                                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<EventTransformation> transformationById(String id) {
        return transformations()
                .thenApply(list -> StreamSupport.stream(list.spliterator(), false)
                                                .filter(t -> id.equals(t.id()))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new));
    }

    @Override
    public CompletableFuture<String> newTransformation(String description) {
        FutureStreamObserver<TransformationId> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.OTHER,
                "An unknown error occurred while starting transformation. No response received from Server.",
                ""
        ));
        stub.startTransformation(StartTransformationRequest.newBuilder().setDescription(description).build(),
                                 responseObserver);
        return responseObserver.thenApply(TransformationId::getId);
    }

    @Override
    public TransformationStream transformationStream(String transformationId) {
        AtomicReference<Consumer<Long>> ackListener = new AtomicReference<>(seq -> {});
        AtomicReference<Consumer<Throwable>> onCompletedByServer = new AtomicReference<>(error -> {});

        StreamObserver<TransformRequestAck> responseObserver =
                responseObserver(ackSequence -> ackListener.get().accept(ackSequence),
                                 error -> onCompletedByServer.get().accept(error));
        return new GrpcTransformationStream(transformationId,
                                            stub.transformEvents(responseObserver),
                                            ackListener::set,
                                            onCompletedByServer::set
        );
    }

    private StreamObserver<TransformRequestAck> responseObserver(Consumer<Long> ackListenerSupplier,
                                                                 Consumer<Throwable> onError) {
        return new StreamObserver<TransformRequestAck>() {
            @Override
            public void onNext(TransformRequestAck value) {
                ackListenerSupplier.accept(value.getSequence());
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("Transformation failed by server", t);
                onError.accept(t);
            }

            @Override
            public void onCompleted() {
                String message = "The server unexpectedly completed the transformation stream";
                NonTransientException e = new NonTransientException(message);
                logger.warn(message, e);
                onError.accept(e);
            }
        };
    }

    @Override
    public CompletableFuture<Void> startApplying(String transformationId,
                                                 Long sequence) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(Empty.getDefaultInstance());
        stub.applyTransformation(applyTransformationRequest(transformationId, sequence), responseObserver);
        return responseObserver.thenAccept(empty -> {});
    }

    @Override
    public CompletableFuture<Void> cancel(String transformationId) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(Empty.getDefaultInstance());
        stub.cancelTransformation(TransformationId.newBuilder()
                                                  .setId(transformationId)
                                                  .build(), responseObserver);
        return responseObserver.thenAccept(empty -> {});
    }

    @Override
    public CompletableFuture<Void> startCompacting() {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(Empty.getDefaultInstance());
        stub.compact(CompactionRequest.newBuilder().build(), responseObserver);
        return responseObserver.thenAccept(empty -> {});
    }
}
