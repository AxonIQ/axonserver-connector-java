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
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.FutureStreamObserver;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.ConsistencyCondition;
import io.axoniq.axonserver.grpc.event.dcb.DcbEventStoreGrpc;
import io.axoniq.axonserver.grpc.event.dcb.ServerSentEvent;
import io.axoniq.axonserver.grpc.event.dcb.StreamQuery;
import io.axoniq.axonserver.grpc.event.dcb.TaggedEvent;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link DcbEventChannel} implementation, serving as the event connection between Axon Server and a client application.
 *
 * @author Milan Savic
 * @since 2024.1.0
 */
public class DcbEventChannelImpl extends AbstractAxonServerChannel<Void> implements DcbEventChannel {

    private static final int BUFFER_SIZE = 32;
    private static final int REFILL_BATCH = 8;
    private final DcbEventStoreGrpc.DcbEventStoreStub eventStore;
    private final ClientIdentification clientIdentification;

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
        this.clientIdentification = clientIdentification;
    }

    @Override
    public void connect() {
        // nothing to do here
    }

    @Override
    public void reconnect() {
        // nothing to do here
    }

    @Override
    public void disconnect() {
        // nothing to do here
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public AppendTransaction startTransaction() {
        FutureStreamObserver<AppendEventsResponse> result = new FutureStreamObserver<>(null);
        StreamObserver<AppendEventsRequest> clientStream = eventStore.appendEvent(result);
        return new AppendTransactionImpl(clientStream, result);
    }

    @Override
    public ResultStream<ServerSentEvent> events(StreamQuery query) {
        AbstractBufferedStream<ServerSentEvent, Empty> result =
                new AbstractBufferedStream<ServerSentEvent, Empty>(clientIdentification.getClientId(),
                                                                   BUFFER_SIZE,
                                                                   REFILL_BATCH) {
                    @Override
                    protected Empty buildFlowControlMessage(FlowControl flowControl) {
                        return null;
                    }

                    @Override
                    protected ServerSentEvent terminalMessage() {
                        return ServerSentEvent.newBuilder()
                                              .build();
                    }
                };
        eventStore.events(query, result);
        return result;
    }

    private static class AppendTransactionImpl implements AppendTransaction {

        private final StreamObserver<AppendEventsRequest> stream;
        private final CompletableFuture<AppendEventsResponse> result;
        private final AtomicBoolean conditionSet = new AtomicBoolean(false);

        AppendTransactionImpl(StreamObserver<AppendEventsRequest> stream,
                              CompletableFuture<AppendEventsResponse> result) {
            this.stream = stream;
            this.result = result;
        }

        @Override
        public AppendTransaction condition(ConsistencyCondition condition) {
            if (conditionSet.compareAndSet(false, true)) {
                stream.onNext(AppendEventsRequest.newBuilder()
                                                 .setCondition(condition)
                                                 .build());
                return this;
            }
            throw new IllegalStateException("Consistency Condition already set.");
        }

        @Override
        public AppendTransaction append(TaggedEvent taggedEvent) {
            stream.onNext(AppendEventsRequest.newBuilder()
                                             .setEvent(taggedEvent)
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
