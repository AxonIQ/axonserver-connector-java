/*
 * Copyright (c) 2020-2026. AxonIQ
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
import io.axoniq.axonserver.connector.event.SnapshottedDcbEventChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.event.dcb.SnapshottedDcbEventStoreGrpc;
import io.axoniq.axonserver.grpc.event.dcb.SnapshottedSourceEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.SnapshottedSourceRequest;

import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link SnapshottedDcbEventChannel} implementation, serving as the snapshot-aware sourcing connection between Axon
 * Server and a client application.
 *
 * @author Milan Savic
 * @since 2026.0.0
 */
public class SnapshottedDcbEventChannelImpl extends AbstractAxonServerChannel<Void>
        implements SnapshottedDcbEventChannel {

    private static final int BUFFER_SIZE = 512;
    private static final int REFILL_BATCH = 16;

    private final SnapshottedDcbEventStoreGrpc.SnapshottedDcbEventStoreStub eventStore;
    private final ClientIdentification clientIdentification;

    /**
     * Instantiate a {@link SnapshottedDcbEventChannelImpl}.
     *
     * @param clientIdentification     the identification of the client
     * @param executor                 a {@link ScheduledExecutorService} used to schedule reconnections
     * @param axonServerManagedChannel the {@link AxonServerManagedChannel} used to connect to AxonServer
     */
    public SnapshottedDcbEventChannelImpl(ClientIdentification clientIdentification,
                                          ScheduledExecutorService executor,
                                          AxonServerManagedChannel axonServerManagedChannel) {
        super(clientIdentification, executor, axonServerManagedChannel);
        this.eventStore = SnapshottedDcbEventStoreGrpc.newStub(axonServerManagedChannel);
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
    public ResultStream<SnapshottedSourceEventsResponse> source(SnapshottedSourceRequest request) {
        AbstractBufferedStream<SnapshottedSourceEventsResponse, Empty> result =
                new AbstractBufferedStream<>(clientIdentification.getClientId(),
                                             BUFFER_SIZE,
                                             REFILL_BATCH) {
                    @Override
                    protected SnapshottedSourceEventsResponse terminalMessage() {
                        return SnapshottedSourceEventsResponse.newBuilder()
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
}
