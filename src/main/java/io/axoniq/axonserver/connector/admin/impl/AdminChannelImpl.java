/*
 * Copyright (c) 2021. AxonIQ
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

package io.axoniq.axonserver.connector.admin.impl;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.FutureStreamObserver;
import io.axoniq.axonserver.grpc.admin.EventProcessorAdminServiceGrpc;
import io.axoniq.axonserver.grpc.admin.EventProcessorAdminServiceGrpc.EventProcessorAdminServiceStub;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;
import io.axoniq.axonserver.grpc.control.ClientIdentification;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nonnull;

/**
 * {@link AdminChannel} GRPC implementation to allow a client application sending
 * and receiving administration related messages to and from Axon Server.
 *
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class AdminChannelImpl extends AbstractAxonServerChannel<Void> implements AdminChannel {

    private final EventProcessorAdminServiceStub eventProcessorServiceStub;

    public AdminChannelImpl(ClientIdentification clientIdentification,
                            ScheduledExecutorService executor, AxonServerManagedChannel channel) {
        super(clientIdentification, executor, channel);
        eventProcessorServiceStub = EventProcessorAdminServiceGrpc.newStub(channel);
    }

    @Override
    public CompletableFuture<Void> pauseEventProcessor(String eventProcessorName, String tokenStoreIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        eventProcessorServiceStub.pauseEventProcessor(eventProcessorIdentifier, responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Override
    public CompletableFuture<Void> startEventProcessor(String eventProcessorName, String tokenStoreIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        eventProcessorServiceStub.startEventProcessor(eventProcessorIdentifier, responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }


    @Override
    public CompletableFuture<Void> splitEventProcessor(String eventProcessorName, String tokenStoreIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        eventProcessorServiceStub.splitEventProcessor(eventProcessorIdentifier, responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Override
    public CompletableFuture<Void> mergeEventProcessor(String eventProcessorName, String tokenStoreIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        eventProcessorServiceStub.mergeEventProcessor(eventProcessorIdentifier, responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Nonnull
    private EventProcessorIdentifier eventProcessorId(String eventProcessorName, String tokenStoreIdentifier) {
        return EventProcessorIdentifier.newBuilder()
                                       .setProcessorName(eventProcessorName)
                                       .setTokenStoreIdentifier(tokenStoreIdentifier)
                                       .build();
    }

    @Override
    public void connect() {
        // there is no stream for the admin channel (yet)
    }

    @Override
    public void reconnect() {
        // there is no stream for the admin channel (yet)
    }

    @Override
    public void disconnect() {
        // there is no stream for the admin channel (yet)
    }

    @Override
    public boolean isReady() {
        return true;
    }
}
