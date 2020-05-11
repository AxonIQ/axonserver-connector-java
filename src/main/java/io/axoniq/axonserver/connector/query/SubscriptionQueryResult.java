package io.axoniq.axonserver.connector.query;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;

import java.util.concurrent.CompletableFuture;

public interface SubscriptionQueryResult {

    CompletableFuture<QueryResponse> initialResult();

    ResultStream<QueryUpdate> updates();
}
