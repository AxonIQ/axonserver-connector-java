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

package io.axoniq.axonserver.connector.admin;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.admin.ApplicationOverview;
import io.axoniq.axonserver.grpc.admin.ApplicationRequest;
import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.axonserver.grpc.admin.ContextUpdate;
import io.axoniq.axonserver.grpc.admin.CreateContextRequest;
import io.axoniq.axonserver.grpc.admin.CreateOrUpdateUserRequest;
import io.axoniq.axonserver.grpc.admin.CreateReplicationGroupRequest;
import io.axoniq.axonserver.grpc.admin.DeleteContextRequest;
import io.axoniq.axonserver.grpc.admin.DeleteReplicationGroupRequest;
import io.axoniq.axonserver.grpc.admin.JoinReplicationGroup;
import io.axoniq.axonserver.grpc.admin.LeaveReplicationGroup;
import io.axoniq.axonserver.grpc.admin.ReplicationGroupOverview;
import io.axoniq.axonserver.grpc.admin.Token;
import io.axoniq.axonserver.grpc.admin.UserOverview;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Communication channel with AxonServer for Administration related interactions.
 *
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public interface AdminChannel {

    /**
     * Request to pause a specific event processor.
     * Returns a {@link CompletableFuture} that completes when the request has been received by AxonServer.
     * This doesn't imply that the event processor has been paused already, but only that the request has been properly
     * delivered.
     *
     * @param eventProcessorName   the name of the event processor to pause
     * @param tokenStoreIdentifier the token store identifier of the processor to pause
     * @return a {@link CompletableFuture} that completes when the request has been delivered to AxonServer
     */
    CompletableFuture<Void> pauseEventProcessor(String eventProcessorName, String tokenStoreIdentifier);

    /**
     * Request to start a specific event processor.
     * Returns a {@link CompletableFuture} that completes when the request has been received by AxonServer.
     * This doesn't imply that the event processor has been started already, but only that the request has been properly
     * delivered.
     *
     * @param eventProcessorName   the name of the event processor to start
     * @param tokenStoreIdentifier the token store identifier of the processor to start
     * @return a {@link CompletableFuture} that completes when the request has been delivered to AxonServer
     */
    CompletableFuture<Void> startEventProcessor(String eventProcessorName, String tokenStoreIdentifier);

    /**
     * Request to split the biggest segment of a specific event processor.
     * Returns a {@link CompletableFuture} that completes when the request has been received by AxonServer.
     * This doesn't imply that the segment has been split already, but only that the request has been properly
     * delivered.
     *
     * @param eventProcessorName   the name of the event processor to split
     * @param tokenStoreIdentifier the token store identifier of the processor to split
     * @return a {@link CompletableFuture} that completes when the request has been delivered to AxonServer
     */
    CompletableFuture<Void> splitEventProcessor(String eventProcessorName, String tokenStoreIdentifier);

    /**
     * Request to merge the two smallest segments of a specific event processor.
     * Returns a {@link CompletableFuture} that completes when the request has been received by AxonServer.
     * This doesn't imply that the segments has been merged already, but only that the request has been properly
     * delivered.
     *
     * @param eventProcessorName   the name of the event processor to merge
     * @param tokenStoreIdentifier the token store identifier of the processor to merge
     * @return a {@link CompletableFuture} that completes when the request has been delivered to AxonServer
     */
    CompletableFuture<Void> mergeEventProcessor(String eventProcessorName, String tokenStoreIdentifier);

    /**
     * Request to create Axon Server user.
     * Returns a {@link CompletableFuture} that completes when the request has been processed by AxonServer.
     *
     * @param request the request to create the user, containing the username, password and user roles
     * @return a {@link CompletableFuture} that completes when the request has been processed to AxonServer
     */
    CompletableFuture<Void> createOrUpdateUser(CreateOrUpdateUserRequest request);

    /**
     * Request to list all users in Axon Server.
     *
     * @return a {@link CompletableFuture} that contains list of all users in Axon Server.
     */
    CompletableFuture<List<UserOverview>> getAllUsers();

    /**
     * Request to delete user in Axon Server.
     *
     * @param username is the username of the user to delete
     * @return a {@link CompletableFuture} that completes when user has been deleted
     */
    CompletableFuture<Void> deleteUser(String username);

    /**
     * Request to create Axon Server application.
     * Returns a {@link CompletableFuture} that completes when the request has been processed by AxonServer.
     *
     * @param request the request to create the application, containing the application name, the application roles...
     * @return a {@link CompletableFuture} that completes when the request has been processed to AxonServer
     */
    CompletableFuture<Void> createOrUpdateApplication(ApplicationRequest request);

    /**
     * Request to retrieve application overview in Axon Server.
     *
     * @param applicationName is the name of the application to retrieve
     * @return a {@link CompletableFuture} containing application data such as name, roles, etc.
     */
    CompletableFuture<ApplicationOverview> getApplication(String applicationName);

    /**
     * Request to refresh token of application in Axon Server.
     *
     * @param applicationName is the name of the application to refresh token
     * @return a {@link CompletableFuture} containing new token
     */
    CompletableFuture<Token> refreshToken(String applicationName);

    /**
     * Request to delete application in Axon Server.
     *
     * @param applicationName is the name of the application to delete
     * @return a {@link CompletableFuture} that completes when application has been deleted
     */
    CompletableFuture<Void> deleteApplication(String applicationName);

    /**
     * Request to create Axon Server context.
     * Returns a {@link CompletableFuture} that completes when the request has been processed by AxonServer.
     *
     * @param request the request to create the context, containing the context name, the context roles...
     * @return a {@link CompletableFuture} that completes when the request has been processed to AxonServer
     */
    CompletableFuture<Void> createContext(CreateContextRequest request);

    /**
     * Request to delete context in Axon Server.
     *
     * @param request the request to delete the context, containing the context name and option to preserve event store.
     * @return a {@link CompletableFuture} that completes when context has been deleted
     */
    CompletableFuture<Void> deleteContext(DeleteContextRequest request);

    /**
     * Request to retrieve context overview in Axon Server.
     *
     * @param context is the name of the context to retrieve
     * @return a {@link CompletableFuture} containing context data such as name, roles, etc.
     */
    CompletableFuture<ContextOverview> getContextOverview(String context);

    /**
     * Request to list all contexts in Axon Server.
     *
     * @return a {@link CompletableFuture} that contains list of all contexts in Axon Server.
     */
    CompletableFuture<List<ContextOverview>> getAllContexts();

    /**
     * Subscribes and listens to context updates from Axon Server, like when a context is created or deleted.
     *
     * @return a {@link ResultStream} streams context updates.
     */
    ResultStream<ContextUpdate> subscribeContextUpdates();

    /**
     * Request to create replication group.
     * Returns a {@link CompletableFuture} that completes when the request has been processed by AxonServer.
     *
     * @param request the request to create replication group,
     * containing the replication group name, the node name, role...
     * @return a {@link CompletableFuture} that completes when the request has been processed to AxonServer
     */
    CompletableFuture<Void> createReplicationGroup(CreateReplicationGroupRequest request);

    /**
     * Request to delete replication group in Axon Server.
     *
     * @param request the request to delete the replication group,
     * containing the replication group name and option to preserve event store.
     * @return a {@link CompletableFuture} that completes when context has been deleted
     */
    CompletableFuture<Void> deleteReplicationGroup(DeleteReplicationGroupRequest request);

    /**
     * Request to retrieve replication group overview in Axon Server.
     *
     * @param replicationGroup is the name of the replication group to retrieve
     * @return a {@link CompletableFuture} containing replication group data such as members, contexts roles, etc.
     */
    CompletableFuture<ReplicationGroupOverview> getReplicationGroup(String replicationGroup);

    /**
     * Request to list all replication groups in Axon Server.
     *
     * @return a {@link CompletableFuture} that contains list of all replication group in Axon Server.
     */
    CompletableFuture<List<ReplicationGroupOverview>> getAllReplicationGroups();

    /**
     * Request to add a node to replication group.
     * Returns a {@link CompletableFuture} that completes when the request has been processed by AxonServer.
     *
     * @param request the request to add a node to replication group,
     * containing the replication group name, the node name, role...
     * @return a {@link CompletableFuture} that completes when the request has been processed to AxonServer
     */
    CompletableFuture<Void> addNodeToReplicationGroup(JoinReplicationGroup request);

    /**
     * Request to remove a node from replication group.
     * Returns a {@link CompletableFuture} that completes when the request has been processed by AxonServer.
     *
     * @param request the request to add a node to replication group,
     * containing the replication group name and option to preserve event store.
     * @return a {@link CompletableFuture} that completes when the request has been processed to AxonServer
     */
    CompletableFuture<Void> removeNodeFromReplicationGroup(LeaveReplicationGroup request);


}
