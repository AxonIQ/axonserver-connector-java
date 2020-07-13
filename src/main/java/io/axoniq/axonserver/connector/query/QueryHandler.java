package io.axoniq.axonserver.connector.query;

import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;

/**
 * Interface representing a component that can handle queries
 */
@FunctionalInterface
public interface QueryHandler {

    /**
     * Handle the given {@code query}, using given {@code responseHandler} to send the response(s).
     * <p>
     * Note that the query <em>must</em> be completed using {@link ResponseHandler#complete()}
     * or {@link ResponseHandler#sendLastResponse(QueryResponse)}.
     *
     * @param query           The message representing the query reqyest
     * @param responseHandler To send the responses
     */
    void handle(QueryRequest query, ResponseHandler responseHandler);

    /**
     * Registers an incoming subscription query request, represented by given {@code query}, using given
     * {@code updateHandler} to send updates when the projection for this query changes.
     * <p>
     * If this handler doesn't support subscription queries for the given {@code query}, it should return {@code null}.
     * Otherwise, it must return a handle that can be used to cancel the subscription query.
     *
     * @param query         The message representing the query
     * @param updateHandler To send updates
     *
     * @return a registration to cancel the subscription, or {@code null} if this handler doesn't support the
     * subscription query.
     */
    default Registration registerSubscriptionQuery(SubscriptionQuery query, UpdateHandler updateHandler) {
        return null;
    }

    /**
     * Interface describing a stream to send responses to queries. These streams must be completed using {@link
     * #complete()} or {@link #sendLastResponse(QueryResponse)} to avoid permit leakage. Components may send as many
     * response messages as desired.
     */
    interface ResponseHandler {

        /**
         * Sends the given {@code response}.
         *
         * @param response The respnose to the query to send
         */
        void sendResponse(QueryResponse response);

        /**
         * Marks the query as completed, possibly signalling flow control that more query messages may be sent.
         * <p>
         * No more responses should be sent after invoking this method. The behavior in that case is undefined, and
         * these messages are likely to be ignored.
         */
        void complete();


        /**
         * Sends the given {@code response}  and arks the query as completed, possibly signalling flow control that
         * more query messages may be sent.
         * <p>
         * No more responses should be sent after invoking this method. The behavior in that case is undefined, and
         * these messages are likely to be ignored.
         */
        default void sendLastResponse(QueryResponse response) {
            try {
                sendResponse(response);
            } finally {
                complete();
            }
        }

    }

    /**
     * Interface describing a stream of updates to a subscription query.
     */
    interface UpdateHandler {

        /**
         * Send the given {@code queryUpdate} in response to the subscription query this handler was provided for.
         *
         * @param queryUpdate The update to send
         */
        void sendUpdate(QueryUpdate queryUpdate);

        /**
         * Indicates the subscription query has completed, meaning no more updates are to be expected. The component
         * sending the subscription query is requested to close the subscription.
         */
        void complete();

    }
}
