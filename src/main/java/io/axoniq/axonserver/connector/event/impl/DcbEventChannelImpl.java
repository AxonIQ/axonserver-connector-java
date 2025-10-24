/*
 * Copyright (c) 2020-2024. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.event.impl;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.FutureStreamObserver;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.event.dcb.AddTagsRequest;
import io.axoniq.axonserver.grpc.event.dcb.AddTagsResponse;
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.CancelScheduledEventRequest;
import io.axoniq.axonserver.grpc.event.dcb.ConsistencyCondition;
import io.axoniq.axonserver.grpc.event.dcb.DcbEventSchedulerGrpc;
import io.axoniq.axonserver.grpc.event.dcb.DcbEventStoreGrpc;
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.GetHeadRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetHeadResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetSequenceAtRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetSequenceAtResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetTagsRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetTagsResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetTailRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetTailResponse;
import io.axoniq.axonserver.grpc.event.dcb.RemoveTagsRequest;
import io.axoniq.axonserver.grpc.event.dcb.RemoveTagsResponse;
import io.axoniq.axonserver.grpc.event.dcb.RescheduleEventRequest;
import io.axoniq.axonserver.grpc.event.dcb.ScheduleEventRequest;
import io.axoniq.axonserver.grpc.event.dcb.ScheduleToken;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.Tag;
import io.axoniq.axonserver.grpc.event.dcb.TaggedEvent;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * {@link DcbEventChannel} implementation, serving as the event connection between Axon Server and a client
 * application.
 *
 * @author Milan Savic
 * @since 2025.1.0
 */
public class DcbEventChannelImpl extends AbstractAxonServerChannel<Void> implements DcbEventChannel {

    private static final int BUFFER_SIZE = 512;
    private static final int REFILL_BATCH = 16;
    private final DcbEventStoreGrpc.DcbEventStoreStub eventStore;
    private final DcbEventSchedulerGrpc.DcbEventSchedulerStub eventScheduler;
    private final ClientIdentification clientIdentification;
    private final Set<ResultStream<StreamEventsResponse>> buffers = ConcurrentHashMap.newKeySet();

    /**
     * Instantiate an {@link AbstractAxonServerChannel}.
     *
     * @param clientIdentification     the identification of the client
     * @param executor                 a {@link ScheduledExecutorService} used to schedule reconnections
     * @param axonServerManagedChannel the {@link AxonServerManagedChannel} used to connect to AxonServer
     */
    public DcbEventChannelImpl(ClientIdentification clientIdentification,
                               ScheduledExecutorService executor,
                               AxonServerManagedChannel axonServerManagedChannel) {
        super(clientIdentification, executor, axonServerManagedChannel);
        this.eventStore = DcbEventStoreGrpc.newStub(axonServerManagedChannel);
        this.eventScheduler = DcbEventSchedulerGrpc.newStub(axonServerManagedChannel);
        this.clientIdentification = clientIdentification;
    }

    @Override
    public void connect() {
        // nothing to do here
    }

    @Override
    public void reconnect() {
        closeBuffers();
    }

    @Override
    public void disconnect() {
        closeBuffers();
    }

    private void closeBuffers() {
        buffers.forEach(ResultStream::close);
        buffers.clear();
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public AppendEventsTransaction startTransaction() {
        FutureStreamObserver<AppendEventsResponse> response = new FutureStreamObserver<>(null);
        StreamObserver<AppendEventsRequest> clientStream = eventStore.append(response);
        return new AppendEventsTransactionImpl(clientStream, response);
    }

    @Override
    public AppendEventsTransaction startTransaction(ConsistencyCondition condition) {
        FutureStreamObserver<AppendEventsResponse> response = new FutureStreamObserver<>(null);
        StreamObserver<AppendEventsRequest> clientStream = eventStore.append(response);
        return new AppendEventsTransactionImpl(clientStream, response).condition(condition);
    }

    @Override
    public ResultStream<StreamEventsResponse> stream(StreamEventsRequest request, int bufferSize, int refillBatch) {
        AbstractBufferedStream<StreamEventsResponse, Empty> buffer =
                new AbstractBufferedStream<>(clientIdentification.getClientId(),
                                             Math.max(64, bufferSize),
                                             Math.max(16, Math.min(bufferSize, refillBatch))) {
                    @Override
                    protected Empty buildFlowControlMessage(FlowControl flowControl) {
                        return null;
                    }

                    @Override
                    protected StreamEventsResponse terminalMessage() {
                        return StreamEventsResponse.getDefaultInstance();
                    }
                };
        buffers.add(buffer);
        buffer.onCloseRequested(() -> buffers.remove(buffer));
        try {
            eventStore.stream(request, buffer);
        } catch (Exception e) {
            buffers.remove(buffer);
            throw e;
        }
        return buffer;
    }

    @Override
    public ResultStream<SourceEventsResponse> source(SourceEventsRequest request) {
        AbstractBufferedStream<SourceEventsResponse, Empty> result =
                new AbstractBufferedStream<SourceEventsResponse, Empty>(clientIdentification.getClientId(),
                                                                        BUFFER_SIZE,
                                                                        REFILL_BATCH) {
                    @Override
                    protected SourceEventsResponse terminalMessage() {
                        return SourceEventsResponse.newBuilder()
                                                   .build();
                    }

                    @Override
                    protected Empty buildFlowControlMessage(FlowControl flowControl) {
                        return null;
                    }
                };
        eventStore.source(request, result);
        return result;
    }

    @Override
    public CompletableFuture<GetTagsResponse> tagsFor(long sequence) {
        FutureStreamObserver<GetTagsResponse> future = new FutureStreamObserver<>(null);
        eventStore.getTags(GetTagsRequest.newBuilder()
                                         .setSequence(sequence)
                                         .build(),
                           future);
        return future;
    }

    @Override
    public CompletableFuture<AppendEventsResponse> append(Collection<TaggedEvent> taggedEvents,
                                                          ConsistencyCondition condition) {
        if (taggedEvents == null || taggedEvents.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("taggedEvents must not be null or empty"));
        }
        FutureStreamObserver<AppendEventsResponse> response = new FutureStreamObserver<>(null);
        StreamObserver<AppendEventsRequest> clientStream = eventStore.append(response);
        return new AppendEventsTransactionImpl(clientStream, response)
                .append(taggedEvents, condition)
                .commit();
    }

    @Override
    public CompletableFuture<GetHeadResponse> head() {
        FutureStreamObserver<GetHeadResponse> future = new FutureStreamObserver<>(null);
        eventStore.getHead(GetHeadRequest.getDefaultInstance(), future);
        return future;
    }

    @Override
    public CompletableFuture<GetTailResponse> tail() {
        FutureStreamObserver<GetTailResponse> future = new FutureStreamObserver<>(null);
        eventStore.getTail(GetTailRequest.getDefaultInstance(), future);
        return future;
    }

    @Override
    public CompletableFuture<GetSequenceAtResponse> getSequenceAt(Instant timestamp) {
        FutureStreamObserver<GetSequenceAtResponse> future = new FutureStreamObserver<>(null);
        eventStore.getSequenceAt(GetSequenceAtRequest.newBuilder()
                                                     .setTimestamp(timestamp.toEpochMilli())
                                                     .build(),
                                 future);
        return future;
    }

    @Override
    public CompletableFuture<AddTagsResponse> addTags(long sequence, Collection<Tag> tags) {
        FutureStreamObserver<AddTagsResponse> future = new FutureStreamObserver<>(null);
        AddTagsRequest request = AddTagsRequest.newBuilder()
                                               .setSequence(sequence)
                                               .addAllTag(tags)
                                               .build();
        eventStore.addTags(request, future);
        return future;
    }

    @Override
    public CompletableFuture<RemoveTagsResponse> removeTags(long sequence, Collection<Tag> tags) {
        FutureStreamObserver<RemoveTagsResponse> future = new FutureStreamObserver<>(null);
        RemoveTagsRequest request = RemoveTagsRequest.newBuilder()
                                                     .setSequence(sequence)
                                                     .addAllTag(tags)
                                                     .build();
        eventStore.removeTags(request, future);
        return future;
    }

    @Override
    public CompletableFuture<String> scheduleEvent(Instant scheduleTime, Event event) {
        FutureStreamObserver<ScheduleToken>responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.INSTRUCTION_ACK_ERROR,
                "An unknown error occurred while scheduling an Event. No response received from Server.",
                ""
        ));

        eventScheduler.scheduleEvent(ScheduleEventRequest.newBuilder()
                                                                 .setEvent(event)
                                                                 .setInstant(scheduleTime.toEpochMilli())
                                                                 .build(), responseObserver);
        return responseObserver.thenApply(ScheduleToken::getToken);
    }

    @Override
    public CompletableFuture<InstructionAck> cancelSchedule(String token) {
        FutureStreamObserver<InstructionAck> responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.INSTRUCTION_ACK_ERROR,
                "An unknown error occurred while cancelling a scheduled Event. No response received from Server.",
                ""
        ));

        eventScheduler.cancelScheduledEvent(CancelScheduledEventRequest.newBuilder().setToken(token).build(),
                                            responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<String> reschedule(String scheduleToken, Instant scheduleTime, @Nullable Event event) {
        FutureStreamObserver<ScheduleToken>responseObserver = new FutureStreamObserver<>(new AxonServerException(
                ErrorCategory.INSTRUCTION_ACK_ERROR,
                "An unknown error occurred while rescheduling Event. No response received from Server.",
                ""
        ));

        RescheduleEventRequest.Builder builder = RescheduleEventRequest.newBuilder()
                                                                       .setToken(scheduleToken)
                                                                       .setInstant(scheduleTime.toEpochMilli());
        if (event != null) {
            builder.setEvent(event);
        }
        eventScheduler.rescheduleEvent(builder.build(), responseObserver);
        return responseObserver.thenApply(ScheduleToken::getToken);
    }


    private static class AppendEventsTransactionImpl implements AppendEventsTransaction {

        private final StreamObserver<AppendEventsRequest> stream;
        private final CompletableFuture<AppendEventsResponse> result;
        private final AtomicBoolean conditionSet = new AtomicBoolean(false);

        AppendEventsTransactionImpl(StreamObserver<AppendEventsRequest> stream,
                                    CompletableFuture<AppendEventsResponse> result) {
            this.stream = stream;
            this.result = result;
        }

        /**
         * Appends this {@code taggedEvent} with {@code condition} to this transaction. Only valid if no consistency
         * condition was supplied before
         *
         * @param taggedEvent the events to be appended
         * @param condition   the consistency condition
         * @return this Transaction for fluency
         * @throws IllegalStateException Will throw an IllegalStateException if Consistency Condition is already set.
         */

        private AppendEventsTransaction append(Collection<TaggedEvent> taggedEvent, ConsistencyCondition condition)
                throws IllegalStateException {
            stream.onNext(createConsistencyCondition(condition)
                                  .addAllEvent(taggedEvent)
                                  .build());
            return this;
        }

        private AppendEventsRequest.Builder createConsistencyCondition(ConsistencyCondition consistencyCondition)
                throws IllegalStateException {
            if (conditionSet.compareAndSet(false, true)) {
                return AppendEventsRequest.newBuilder()
                                          .setCondition(consistencyCondition);
            }
            throw new IllegalStateException("Consistency Condition already set.");
        }

        public AppendEventsTransaction condition(ConsistencyCondition condition) {
            stream.onNext(createConsistencyCondition(condition).build());
            return this;
        }

        @Override
        public AppendEventsTransaction append(TaggedEvent taggedEvent) {
            stream.onNext(AppendEventsRequest.newBuilder()
                                             .addEvent(taggedEvent)
                                             .build());
            return this;
        }

        @Override
        public CompletableFuture<AppendEventsResponse> commit() {
            stream.onCompleted();
            return result;
        }

        @Override
        public void rollback() {
            stream.onError(new StatusRuntimeException(Status.CANCELLED));
        }
    }
}
