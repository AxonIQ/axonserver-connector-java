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
import io.axoniq.axonserver.grpc.event.dcb.AppendRequest;
import io.axoniq.axonserver.grpc.event.dcb.AppendResponse;
import io.axoniq.axonserver.grpc.event.dcb.ConsistencyCondition;
import io.axoniq.axonserver.grpc.event.dcb.DcbEventStoreGrpc;
import io.axoniq.axonserver.grpc.event.dcb.SourceRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceResponse;
import io.axoniq.axonserver.grpc.event.dcb.StreamRequest;
import io.axoniq.axonserver.grpc.event.dcb.StreamResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link DcbEventChannel} implementation, serving as the event connection between Axon Server and a client
 * application.
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
        FutureStreamObserver<AppendResponse> response = new FutureStreamObserver<>(null);
        StreamObserver<AppendRequest> clientStream = eventStore.append(response);
        return new AppendTransactionImpl(clientStream, response);
    }

    @Override
    public ResultStream<StreamResponse> stream(StreamRequest request) {
        AbstractBufferedStream<StreamResponse, Empty> result =
                new AbstractBufferedStream<StreamResponse, Empty>(clientIdentification.getClientId(),
                                                                  BUFFER_SIZE,
                                                                  REFILL_BATCH) {
                    @Override
                    protected Empty buildFlowControlMessage(FlowControl flowControl) {
                        return null;
                    }

                    @Override
                    protected StreamResponse terminalMessage() {
                        return StreamResponse.newBuilder()
                                             .build();
                    }
                };
        eventStore.stream(request, result);
        return result;
    }

    @Override
    public ResultStream<SourceResponse> source(SourceRequest request) {
        AbstractBufferedStream<SourceResponse, Empty> result = 
                new AbstractBufferedStream<SourceResponse, Empty>(clientIdentification.getClientId(),
                                                                  BUFFER_SIZE,
                                                                  REFILL_BATCH) {
                    @Override
                    protected SourceResponse terminalMessage() {
                        return SourceResponse.newBuilder()
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

    private static class AppendTransactionImpl implements AppendTransaction {

        private final StreamObserver<AppendRequest> stream;
        private final CompletableFuture<AppendResponse> result;
        private final AtomicBoolean conditionSet = new AtomicBoolean(false);

        AppendTransactionImpl(StreamObserver<AppendRequest> stream,
                              CompletableFuture<AppendResponse> result) {
            this.stream = stream;
            this.result = result;
        }

        @Override
        public AppendTransaction condition(ConsistencyCondition condition) {
            if (conditionSet.compareAndSet(false, true)) {
                stream.onNext(AppendRequest.newBuilder()
                                           .setCondition(condition)
                                           .build());
                return this;
            }
            throw new IllegalStateException("Consistency Condition already set.");
        }

        @Override
        public AppendTransaction append(AppendRequest.Event taggedEvent) {
            stream.onNext(AppendRequest.newBuilder()
                                       .setEvent(taggedEvent)
                                       .build());
            return this;
        }

        @Override
        public CompletableFuture<AppendResponse> commit() {
            stream.onCompleted();
            return result;
        }

        @Override
        public void rollback() {
            stream.onError(new StatusRuntimeException(Status.CANCELLED));
        }
    }
}
