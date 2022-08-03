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
import io.axoniq.axonserver.grpc.admin.EventProcessor;
import io.axoniq.axonserver.grpc.admin.JoinReplicationGroup;
import io.axoniq.axonserver.grpc.admin.LeaveReplicationGroup;
import io.axoniq.axonserver.grpc.admin.LoadBalancingStrategy;
import io.axoniq.axonserver.grpc.admin.NodeOverview;
import io.axoniq.axonserver.grpc.admin.ReplicationGroupOverview;
import io.axoniq.axonserver.grpc.admin.Result;
import io.axoniq.axonserver.grpc.admin.Token;
import io.axoniq.axonserver.grpc.admin.UpdateContextPropertiesRequest;
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
     * Returns all the event processor registered to AxonServer.
     *
     * @return the stream of all event processor registered to AxonServer.
     */
    ResultStream<EventProcessor> eventProcessors();

    /**
     * Returns all the event processor registered by the specified application.
     *
     * @param component the component name
     * @return the stream of all event processor registered to AxonServer by the specified application.
     */
    ResultStream<EventProcessor> eventProcessorsByComponent(String component);

    /**
     * Request to pause a specific event processor. Returns a {@link CompletableFuture} that completes when the pause
     * has been performed. The {@link CompletableFuture} completes with {@link Result} {@code ACCEPTED}, if the client
     * application running the event processor is using a version of the connector prior to 4.6.0.
     *
     * @param eventProcessorName   the name of the event processor to pause
     * @param tokenStoreIdentifier the token store identifier of the processor to pause
     * @return a {@link CompletableFuture} that completes when the pause has been performed
     */
    CompletableFuture<Result> pauseEventProcessor(String eventProcessorName, String tokenStoreIdentifier);

    /**
     * Request to start a specific event processor. Returns a {@link CompletableFuture} that completes when the start
     * has been performed
     * The {@link CompletableFuture} completes with {@link Result} {@code ACCEPTED}, if the client application
     * running the event processor is using a version of the connector prior to 4.6.0.
     *
     * @param eventProcessorName   the name of the event processor to start
     * @param tokenStoreIdentifier the token store identifier of the processor to start
     * @return a {@link CompletableFuture} that completes when the start has been performed
     */
    CompletableFuture<Result> startEventProcessor(String eventProcessorName, String tokenStoreIdentifier);

    /**
     * Request to split the biggest segment of a specific event processor. Returns a {@link CompletableFuture} that
     * completes when the split has been performed
     * The {@link CompletableFuture} completes with {@link Result} {@code ACCEPTED}, if the client application
     * running the event processor is using a version of the connector prior to 4.6.0.
     *
     * @param eventProcessorName   the name of the event processor to split
     * @param tokenStoreIdentifier the token store identifier of the processor to split
     * @return a {@link CompletableFuture} that completes when the split has been performed
     */
    CompletableFuture<Result> splitEventProcessor(String eventProcessorName, String tokenStoreIdentifier);

    /**
     * Request to merge the two smallest segments of a specific event processor. Returns a {@link CompletableFuture}
     * that completes when the merge has been performed
     * The {@link CompletableFuture} completes with {@link Result} {@code ACCEPTED}, if the client application
     * running the event processor is using a version of the connector prior to 4.6.0.
     *
     * @param eventProcessorName   the name of the event processor to merge
     * @param tokenStoreIdentifier the token store identifier of the processor to merge
     * @return a {@link CompletableFuture} that completes when the merge has been performed
     */
    CompletableFuture<Result> mergeEventProcessor(String eventProcessorName, String tokenStoreIdentifier);

    /**
     * Request to balance the load for the given {@code eventProcessorName} within the connected client.
     * Returns a {@link CompletableFuture} that completes when the request has been received by Axon Server.
     * Note that this doesn't imply that the processor is balanced, but only that the request has been properly
     * delivered.
     *
     * @param eventProcessorName   the name of the event processor to balance the load for
     * @param tokenStoreIdentifier the token store identifier of the processor to balance the load for
     * @param strategy             the balancing strategy to use
     * @return a {@link CompletableFuture} that completes when the request is delivered to Axon Server
     */
    CompletableFuture<Void> loadBalanceEventProcessor(String eventProcessorName, String tokenStoreIdentifier, String strategy);

    /**
     *  Updates the autoloadbalance strategy for a {@code eventProcessorName} within the connected client.
     * Returns a {@link CompletableFuture} that completes when the request has been received by Axon Server.
     * Note that this doesn't imply that the processor is balanced, but only that the request has been properly
     * delivered.
     *
     * @param eventProcessorName   the name of the event processor to balance the load for
     * @param tokenStoreIdentifier the token store identifier of the processor to balance the load for
     * @param strategy             the balancing strategy to use
     * @return a {@link CompletableFuture} that completes when the request is delivered to Axon Server
     */
    CompletableFuture<Void> setAutoLoadBalanceStrategy(String eventProcessorName, String tokenStoreIdentifier, String strategy);


    /**
     * Returns all available load balance strategies registered to AxonServer.
     *
     * @return the list of all available load balance strategies registered to AxonServer.
     */
    CompletableFuture<List<LoadBalancingStrategy>> getBalancingStrategies();

    /**
     * Requests to move a specific event processor segment to a certain client. Returns a {@link CompletableFuture} that
     * completes when all clients other than the {@code targetClientIdentifier} release or disregard the segment for claiming. There is no guarantee that the target client has
     * already claimed the segment when the result completes.
     *
     * @param eventProcessorName     the name of the event processor to move
     * @param tokenStoreIdentifier   the token store identifier of the processor to move
     * @param segmentId              the identifier of the segment to move
     * @param targetClientIdentifier the desired destination for the segment
     * @return a {@link CompletableFuture} that completes when all the other clients released the segment or disregard the segment for claiming.
     * There is no guarantee that the target client has already claimed the segment when the result completes.
     */
    CompletableFuture<Result> moveEventProcessorSegment(String eventProcessorName,
                                                        String tokenStoreIdentifier,
                                                        int segmentId,
                                                        String targetClientIdentifier);

    /**
     * Request to create an Axon Server user, or update it if it's already present. Returns a {@link CompletableFuture}
     * that completes when the request has been processed by AxonServer.
     *
     * @param request {@link CreateOrUpdateUserRequest} to create the user, containing the username, password and user
     *                roles
     * @return a {@link CompletableFuture} that completes when the request has been processed by Axon Server
     */
    CompletableFuture<Void> createOrUpdateUser(CreateOrUpdateUserRequest request);

    /**
     * Request to list all users in Axon Server.
     *
     * @return a {@link CompletableFuture} that contains list of all {@link UserOverview} in Axon Server.
     */
    CompletableFuture<List<UserOverview>> getAllUsers();

    /**
     * Request to delete a user in Axon Server.
     *
     * @param username is the username of the user to delete
     * @return a {@link CompletableFuture} that completes when the user has been deleted
     */
    CompletableFuture<Void> deleteUser(String username);

    /**
     * Request to create an Axon Server application, or update it if it's already present.
     * Returns a {@link CompletableFuture} that completes when the request has been processed by Axon Server.
     *
     * @param request {@link ApplicationRequest} to create the application
     * @return a {@link CompletableFuture} that contains application {@link Token} and completes
     * when the request has been processed by Axon Server
     */
    CompletableFuture<Token> createOrUpdateApplication(ApplicationRequest request);

    /**
     * Request to retrieve application overview in Axon Server.
     *
     * @param applicationName is the name of the application to retrieve
     * @return a {@link CompletableFuture} containing {@link ApplicationOverview} with data such as name, roles, etc.
     */
    CompletableFuture<ApplicationOverview> getApplication(String applicationName);

    /**
     * Request to list all applications in Axon Server.
     *
     * @return a {@link CompletableFuture} that contains list of all {@link ApplicationOverview} in Axon Server.
     */
    CompletableFuture<List<ApplicationOverview>> getAllApplications();

    /**
     * Request to refresh token of application in Axon Server.
     *
     * @param applicationName is the name of the application to refresh token
     * @return a {@link CompletableFuture} containing new {@link Token}
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
     * Request to create an Axon Server context. Returns a {@link CompletableFuture} that completes when the request has
     * been processed by Axon Server.
     *
     * @param request {@link CreateContextRequest} to create the context
     * @return a {@link CompletableFuture} that completes when the request has been processed by Axon Server
     */
    CompletableFuture<Void> createContext(CreateContextRequest request);

    /**
     * Request to update the properties for an Axon Server context. Returns a {@link CompletableFuture} that completes
     * when the request has been processed by Axon Server.
     *
     * @param request {@link UpdateContextPropertiesRequest} to update the context properties
     * @return a {@link CompletableFuture} that completes when the request has been processed by Axon Server
     */
    CompletableFuture<Void> updateContextProperties(UpdateContextPropertiesRequest request);

    /**
     * Request to delete a context in Axon Server.
     *
     * @param request {@link DeleteContextRequest} to delete the context, containing the context name and option to
     *                preserve event store.
     * @return a {@link CompletableFuture} that completes when context has been deleted
     */
    CompletableFuture<Void> deleteContext(DeleteContextRequest request);

    /**
     * Request to retrieve a {@link ContextOverview} from Axon Server.
     *
     * @param context the name of the {@link ContextOverview} to retrieve
     * @return a {@link CompletableFuture} containing {@link ContextOverview} data such as name, roles, etc.
     */
    CompletableFuture<ContextOverview> getContextOverview(String context);

    /**
     * Request to list all contexts in Axon Server.
     *
     * @return a {@link CompletableFuture} that contains a list of all {@link ContextOverview} in Axon Server.
     */
    CompletableFuture<List<ContextOverview>> getAllContexts();

    /**
     * Subscribes and listens to context updates from Axon Server, like when a context is created or deleted.
     *
     * @return a {@link ResultStream} that streams {@link ContextUpdate}.
     */
    ResultStream<ContextUpdate> subscribeToContextUpdates();

    /**
     * Request to create a replication group.
     * Returns a {@link CompletableFuture} that completes when the request has been processed by Axon Server.
     *
     * @param request {@link CreateReplicationGroupRequest} to create replication group,
     *                containing the replication group name, the node name, role...
     * @return a {@link CompletableFuture} that completes when the request has been processed to AxonServer
     */
    CompletableFuture<Void> createReplicationGroup(CreateReplicationGroupRequest request);

    /**
     * Request to delete a replication group in Axon Server.
     *
     * @param request {@link DeleteReplicationGroupRequest} to delete the replication group,
     *                containing the replication group name and option to preserve event store.
     * @return a {@link CompletableFuture} that completes when the replication group has been deleted
     */
    CompletableFuture<Void> deleteReplicationGroup(DeleteReplicationGroupRequest request);

    /**
     * Request to retrieve replication group overview in Axon Server.
     *
     * @param replicationGroup is the name of the replication group to retrieve
     * @return a {@link CompletableFuture} containing {@link ReplicationGroupOverview} data such as members, contexts
     * roles, etc.
     */
    CompletableFuture<ReplicationGroupOverview> getReplicationGroup(String replicationGroup);

    /**
     * Request to list all replication groups in Axon Server.
     *
     * @return a {@link CompletableFuture} that contains list of all {@link ReplicationGroupOverview} in Axon Server.
     */
    CompletableFuture<List<ReplicationGroupOverview>> getAllReplicationGroups();

    /**
     * Request to list all nodes in Axon Server.
     *
     * @return a {@link CompletableFuture} that contains list of all {@link NodeOverview} in Axon Server.
     */
    CompletableFuture<List<NodeOverview>> getAllNodes();

    /**
     * Request to add a node to a replication group.
     * Returns a {@link CompletableFuture} that completes when the request has been processed by AxonServer.
     *
     * @param request {@link JoinReplicationGroup} to add a node to replication group,
     *                containing the replication group name, the node name, role...
     * @return a {@link CompletableFuture} that completes when the request has been processed to AxonServer
     */
    CompletableFuture<Void> addNodeToReplicationGroup(JoinReplicationGroup request);

    /**
     * Request to remove a node from a replication group.
     * Returns a {@link CompletableFuture} that completes when the request has been processed by Axon Server.
     *
     * @param request {@link LeaveReplicationGroup} to a node to be removed from replication group
     * @return a {@link CompletableFuture} that completes when the request has been processed by Axon Server
     */
    CompletableFuture<Void> removeNodeFromReplicationGroup(LeaveReplicationGroup request);
}
