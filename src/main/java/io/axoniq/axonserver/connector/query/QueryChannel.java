package io.axoniq.axonserver.connector.query;

import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;

public interface QueryChannel {

    ResultStream<QueryResponse> query(QueryRequest query);

    SubscriptionQueryResult subscriptionQuery(QueryRequest query, SerializedObject updateType, int bufferSize, int fetchSize);

    Registration registerQueryHandler(QueryHandler handler, QueryDefinition... queryTypes);

}
