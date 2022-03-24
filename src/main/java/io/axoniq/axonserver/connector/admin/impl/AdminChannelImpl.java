/*
 * Copyright (c) 2022. AxonIQ
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
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.FutureListStreamObserver;
import io.axoniq.axonserver.connector.impl.FutureStreamObserver;
import io.axoniq.axonserver.grpc.Component;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.admin.AdminActionResult;
import io.axoniq.axonserver.grpc.admin.ApplicationAdminServiceGrpc;
import io.axoniq.axonserver.grpc.admin.ApplicationId;
import io.axoniq.axonserver.grpc.admin.ApplicationOverview;
import io.axoniq.axonserver.grpc.admin.ApplicationRequest;
import io.axoniq.axonserver.grpc.admin.ContextAdminServiceGrpc;
import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.axonserver.grpc.admin.ContextUpdate;
import io.axoniq.axonserver.grpc.admin.CreateContextRequest;
import io.axoniq.axonserver.grpc.admin.CreateOrUpdateUserRequest;
import io.axoniq.axonserver.grpc.admin.CreateReplicationGroupRequest;
import io.axoniq.axonserver.grpc.admin.DeleteContextRequest;
import io.axoniq.axonserver.grpc.admin.DeleteReplicationGroupRequest;
import io.axoniq.axonserver.grpc.admin.DeleteUserRequest;
import io.axoniq.axonserver.grpc.admin.EventProcessor;
import io.axoniq.axonserver.grpc.admin.EventProcessorAdminServiceGrpc;
import io.axoniq.axonserver.grpc.admin.EventProcessorAdminServiceGrpc.EventProcessorAdminServiceStub;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;
import io.axoniq.axonserver.grpc.admin.GetContextRequest;
import io.axoniq.axonserver.grpc.admin.GetReplicationGroupRequest;
import io.axoniq.axonserver.grpc.admin.JoinReplicationGroup;
import io.axoniq.axonserver.grpc.admin.LeaveReplicationGroup;
import io.axoniq.axonserver.grpc.admin.MoveSegment;
import io.axoniq.axonserver.grpc.admin.ReplicationGroupAdminServiceGrpc;
import io.axoniq.axonserver.grpc.admin.ReplicationGroupOverview;
import io.axoniq.axonserver.grpc.admin.Result;
import io.axoniq.axonserver.grpc.admin.Token;
import io.axoniq.axonserver.grpc.admin.UserAdminServiceGrpc;
import io.axoniq.axonserver.grpc.admin.UserOverview;
import io.axoniq.axonserver.grpc.control.ClientIdentification;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link AdminChannel} GRPC implementation to allow a client application sending and receiving administration related
 * messages to and from Axon Server.
 *
 * @author Sara Pellegrini
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public class AdminChannelImpl extends AbstractAxonServerChannel<Void> implements AdminChannel {

    private static final int BUFFER_SIZE = 32;
    private static final int REFILL_BATCH = 8;
    private static final Empty EMPTY = Empty.getDefaultInstance();

    private final EventProcessorAdminServiceStub eventProcessorServiceStub;
    private final ContextAdminServiceGrpc.ContextAdminServiceStub contextServiceStub;
    private final ReplicationGroupAdminServiceGrpc.ReplicationGroupAdminServiceStub replicationGroupServiceStub;
    private final ApplicationAdminServiceGrpc.ApplicationAdminServiceStub applicationServiceStub;
    private final UserAdminServiceGrpc.UserAdminServiceStub userServiceStub;

    public AdminChannelImpl(ClientIdentification clientIdentification,
                            ScheduledExecutorService executor, AxonServerManagedChannel channel) {
        super(clientIdentification, executor, channel);
        eventProcessorServiceStub = EventProcessorAdminServiceGrpc.newStub(channel);
        this.contextServiceStub = ContextAdminServiceGrpc.newStub(channel);
        this.replicationGroupServiceStub = ReplicationGroupAdminServiceGrpc.newStub(channel);
        this.applicationServiceStub = ApplicationAdminServiceGrpc.newStub(channel);
        this.userServiceStub = UserAdminServiceGrpc.newStub(channel);
    }

    public ResultStream<EventProcessor> eventProcessors() {
        AbstractBufferedStream<EventProcessor, Empty> results = new AbstractBufferedStream<EventProcessor, Empty>(
                "", BUFFER_SIZE, REFILL_BATCH
        ) {

            @Override
            protected Empty buildFlowControlMessage(FlowControl flowControl) {
                // no app-level flow control available on this request
                return null;
            }

            @Override
            protected EventProcessor terminalMessage() {
                return EventProcessor.newBuilder().build();
            }
        };
        eventProcessorServiceStub.getAllEventProcessors(EMPTY, results);
        return results;
    }

    public ResultStream<EventProcessor> eventProcessorsByComponent(String component) {
        Component request = Component.newBuilder().setComponent(component).build();

        AbstractBufferedStream<EventProcessor, Empty> results = new AbstractBufferedStream<EventProcessor, Empty>(
                "", BUFFER_SIZE, REFILL_BATCH
        ) {

            @Override
            protected Empty buildFlowControlMessage(FlowControl flowControl) {
                // no app-level flow control available on this request
                return null;
            }

            @Override
            protected EventProcessor terminalMessage() {
                return EventProcessor.newBuilder().build();
            }
        };
        eventProcessorServiceStub.getEventProcessorsByComponent(request, results);
        return results;
    }

    @Override
    public CompletableFuture<Result> pauseEventProcessor(String eventProcessorName, String tokenStoreIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        FutureStreamObserver<AdminActionResult> responseObserver = new FutureStreamObserver<>(null);
        eventProcessorServiceStub.pauseEventProcessor(eventProcessorIdentifier, responseObserver);
        return responseObserver.thenApply(AdminActionResult::getResult);
    }

    @Override
    public CompletableFuture<Result> startEventProcessor(String eventProcessorName, String tokenStoreIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        FutureStreamObserver<AdminActionResult> responseObserver = new FutureStreamObserver<>(null);
        eventProcessorServiceStub.startEventProcessor(eventProcessorIdentifier, responseObserver);
        return responseObserver.thenApply(AdminActionResult::getResult);
    }


    @Override
    public CompletableFuture<Result> splitEventProcessor(String eventProcessorName, String tokenStoreIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        FutureStreamObserver<AdminActionResult> responseObserver = new FutureStreamObserver<>(null);
        eventProcessorServiceStub.splitEventProcessor(eventProcessorIdentifier, responseObserver);
        return responseObserver.thenApply(AdminActionResult::getResult);
    }

    @Override
    public CompletableFuture<Result> mergeEventProcessor(String eventProcessorName, String tokenStoreIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        FutureStreamObserver<AdminActionResult> responseObserver = new FutureStreamObserver<>(null);
        eventProcessorServiceStub.mergeEventProcessor(eventProcessorIdentifier, responseObserver);
        return responseObserver.thenApply(AdminActionResult::getResult);
    }

    @Override
    public CompletableFuture<Result> moveEventProcessorSegment(String eventProcessorName, String tokenStoreIdentifier,
                                                               int segmentId, String targetClientIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        FutureStreamObserver<AdminActionResult> responseObserver = new FutureStreamObserver<>(null);
        MoveSegment request = MoveSegment.newBuilder()
                                         .setEventProcessor(eventProcessorIdentifier)
                                         .setSegment(segmentId)
                                         .setTargetClientId(targetClientIdentifier)
                                         .build();
        eventProcessorServiceStub.moveEventProcessorSegment(request, responseObserver);
        return responseObserver.thenApply(AdminActionResult::getResult);
    }

    @Nonnull
    private EventProcessorIdentifier eventProcessorId(String eventProcessorName, String tokenStoreIdentifier) {
        return EventProcessorIdentifier.newBuilder()
                                       .setProcessorName(eventProcessorName)
                                       .setTokenStoreIdentifier(tokenStoreIdentifier)
                                       .build();
    }

    @Override
    public CompletableFuture<Void> createOrUpdateUser(CreateOrUpdateUserRequest request) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        userServiceStub.createOrUpdateUser(request, responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Override
    public CompletableFuture<List<UserOverview>> getAllUsers() {
        FutureListStreamObserver<UserOverview> responseObserver = new FutureListStreamObserver<>();
        userServiceStub.getUsers(Empty.newBuilder().build(), responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<Void> deleteUser(String username) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        userServiceStub.deleteUser(DeleteUserRequest.newBuilder().setUserName(username).build(), responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Override
    public CompletableFuture<Token> createOrUpdateApplication(ApplicationRequest request) {
        FutureStreamObserver<Token> responseObserver = new FutureStreamObserver<>(null);
        applicationServiceStub.createOrUpdateApplication(request, responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<List<ApplicationOverview>> getAllApplications() {
        FutureListStreamObserver<ApplicationOverview> responseObserver = new FutureListStreamObserver<>();
        applicationServiceStub.getApplications(Empty.newBuilder().build(), responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<ApplicationOverview> getApplication(String applicationName) {
        FutureStreamObserver<ApplicationOverview> responseObserver = new FutureStreamObserver<>(null);
        applicationServiceStub.getApplication(ApplicationId.newBuilder().setApplicationName(applicationName).build(),
                                              responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<Token> refreshToken(String applicationName) {
        FutureStreamObserver<Token> responseObserver = new FutureStreamObserver<>(null);
        applicationServiceStub.refreshToken(ApplicationId.newBuilder().setApplicationName(applicationName).build(),
                                            responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<Void> deleteApplication(String applicationName) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        applicationServiceStub.deleteApplication(ApplicationId.newBuilder().setApplicationName(applicationName).build(),
                                                 responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Override
    public CompletableFuture<Void> createContext(CreateContextRequest request) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        contextServiceStub.createContext(request, responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Override
    public CompletableFuture<Void> deleteContext(DeleteContextRequest request) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        contextServiceStub.deleteContext(request, responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Override
    public CompletableFuture<ContextOverview> getContextOverview(String context) {
        FutureStreamObserver<ContextOverview> responseObserver = new FutureStreamObserver<>(null);
        contextServiceStub.getContext(GetContextRequest.newBuilder().setName(context).build(), responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<List<ContextOverview>> getAllContexts() {
        FutureListStreamObserver<ContextOverview> responseObserver = new FutureListStreamObserver<>();
        contextServiceStub.getContexts(Empty.newBuilder().build(), responseObserver);
        return responseObserver;
    }


    @Override
    public ResultStream<ContextUpdate> subscribeToContextUpdates() {
        AbstractBufferedStream<ContextUpdate, Empty> results = new AbstractBufferedStream<ContextUpdate, Empty>(
                "", BUFFER_SIZE, REFILL_BATCH
        ) {

            @Override
            protected Empty buildFlowControlMessage(FlowControl flowControl) {
                // no app-level flow control available on this request
                return null;
            }

            @Override
            protected ContextUpdate terminalMessage() {
                return ContextUpdate.newBuilder().build();
            }
        };
        contextServiceStub.subscribeContextUpdates(Empty.newBuilder().build(), results);
        return results;
    }

    @Override
    public CompletableFuture<Void> addNodeToReplicationGroup(JoinReplicationGroup request) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        replicationGroupServiceStub.addNodeToReplicationGroup(request, responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Override
    public CompletableFuture<Void> createReplicationGroup(CreateReplicationGroupRequest request) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        replicationGroupServiceStub.createReplicationGroup(request, responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Override
    public CompletableFuture<Void> deleteReplicationGroup(DeleteReplicationGroupRequest request) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        replicationGroupServiceStub.deleteReplicationGroup(request, responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Override
    public CompletableFuture<ReplicationGroupOverview> getReplicationGroup(String replicationGroup) {
        FutureStreamObserver<ReplicationGroupOverview> responseObserver = new FutureStreamObserver<>(null);
        replicationGroupServiceStub.getReplicationGroup(GetReplicationGroupRequest
                                                                .newBuilder()
                                                                .setName(replicationGroup)
                                                                .build(), responseObserver);
        return responseObserver;
    }

    @Override
    public CompletableFuture<Void> removeNodeFromReplicationGroup(LeaveReplicationGroup request) {
        FutureStreamObserver<Empty> responseObserver = new FutureStreamObserver<>(null);
        replicationGroupServiceStub.removeNodeFromReplicationGroup(request, responseObserver);
        return responseObserver.thenAccept(empty -> {
        });
    }

    @Override
    public CompletableFuture<List<ReplicationGroupOverview>> getAllReplicationGroups() {
        FutureListStreamObserver<ReplicationGroupOverview> responseObserver = new FutureListStreamObserver<>();
        replicationGroupServiceStub.getReplicationGroups(Empty.newBuilder().build(), responseObserver);
        return responseObserver;
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
