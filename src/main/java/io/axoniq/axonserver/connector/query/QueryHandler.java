package io.axoniq.axonserver.connector.query;

import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;

@FunctionalInterface
public interface QueryHandler {

    void handle(QueryRequest query, ResponseHandler responseHandler);

    default Registration registerSubscriptionQuery(QueryRequest query, UpdateHandler sendUpdate) {
        return null;
    }

    interface ResponseHandler {

        void sendResponse(QueryResponse response);

        void complete();

        default void sendLastResponse(QueryResponse response) {
            try {
                sendResponse(response);
            } finally {
                complete();
            }
        }

    }

    interface UpdateHandler {

        void sendUpdate(QueryUpdate response);

        void complete();

    }
}
