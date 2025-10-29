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

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;

import java.util.concurrent.CompletableFuture;

/**
 * Interface describing the results of a subscription query. This type of query consists of an initial response,
 * representing the state of a projection at the start of the query, and a stream of updates which represent the updates
 * to the model since the query started.
 */
public interface SubscriptionQueryResult {

    /**
     * Returns a CompletableFuture that completes when the initial result of the query is available. If an error
     * occurred while querying, the CompletableFuture completes exceptionally.
     * <p>
     * Invoking this method will send a request for the initial result, if that hasn't been requested before. Subsequent
     * invocations of this method will return the same CompletableFuture instance.
     * <p>
     *     Note that calling this method will not prevent the {@link #initialResults()} call from retrieving a new set
     *     of initial results, or vice versa.
     *
     * @return a CompletableFuture that completes with the initial result of the subscription query
     * @deprecated in favor of {@link #initialResults()}, which returns a stream of results.
     */
    @Deprecated(since = "2025.2.0", forRemoval = true)
    CompletableFuture<QueryResponse> initialResult();

    /**
     * Returns a ResultStream that represent the initial result of the subscription query.
     * <p>
     * Invoking this method will send a request for the initial result if that hasn't been requested before. Further
     * invocations of this method will return the same ResultStream instance.
     *
     * @return a ResultStream that provides the initial result of the subscription query
     */
    ResultStream<QueryResponse> initialResults();

    /**
     * Returns the stream of updates to the queried projection. The stream can be read in a blocking and non-blocking
     * fashion, as desired by downstream processes.
     * <p>
     * Multiple invocation of this method will return the same stream instance.
     *
     * @return a stream of updates
     */
    ResultStream<QueryUpdate> updates();
}
