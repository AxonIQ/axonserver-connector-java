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
     *
     * @return a CompletableFuture that completes with the initial result of the subscription query
     */
    CompletableFuture<QueryResponse> initialResult();

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
