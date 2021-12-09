/*
 * Copyright (c) 2020. AxonIQ
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

package io.axoniq.axonserver.connector.query;

import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Communication channel with AxonServer for Query related interactions.
 */
public interface QueryChannel {

    /**
     * Registers the given {@code handler} to handle incoming queries defined through the given {@code queryTypes}.
     * Duplicate {@code handler}s can be registered.
     *
     * @param handler    the handler to handle incoming queries with
     * @param queryTypes the {@link QueryDefinition}s to register the handler for
     * @return a registration which allows the query handler to be unregistered
     */
    Registration registerQueryHandler(QueryHandler handler, QueryDefinition... queryTypes);

    /**
     * Sends the given {@code query} to AxonServer for routing to the appropriate handlers.
     *
     * @param query the {@link QueryRequest} to send
     * @return a {@link ResultStream} providing the results of query execution
     */
    ResultStream<QueryResponse> query(QueryRequest query);

    /**
     * Sends out a subscription {@code query} to AxonServer for routing to the appropriate handler. Allows for receiving
     * an initial query result followed by a stream of updates.
     *
     * @param query      the subscription {@link QueryRequest} to send
     * @param updateType the type of updates expected from this subscription query
     * @param bufferSize the number of updates to be buffered by the update result stream
     * @param fetchSize  the number of updates to be consumed from the update stream prior to refilling it
     * @return the {@link SubscriptionQueryResult} containing the initial result and update stream
     */
    SubscriptionQueryResult subscriptionQuery(QueryRequest query,
                                              SerializedObject updateType,
                                              int bufferSize,
                                              int fetchSize);

    /**
     * Prepares this {@link QueryChannel} to disconnect, by unsubscribing all registered query handlers. Will wait with
     * a certain cut off until all acknowledgments of unsubscribing have been received.
     * <p>
     * This method should be used if a connected client wants to disconnect from AxonServer.
     *
     * @return a {@link CompletableFuture} of {@link Void} to react when all query handlers have been unsubscribed
     */
    CompletableFuture<Void> prepareDisconnect();
}
