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

import io.axoniq.axonserver.connector.FlowControl;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.impl.NoopFlowControl;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;

/**
 * Interface representing a component that can handle queries.
 */
@FunctionalInterface
public interface QueryHandler {

    /**
     * Handle the given {@code query}, using given {@code responseHandler} to send the response(s).
     * <p>
     * Note that the query <em>must</em> be completed using {@link ReplyChannel#complete()} or {@link
     * ReplyChannel#sendLast(Object)}.
     *
     * @param query           the message representing the query request
     * @param responseHandler to handler to send responses with
     */
    void handle(QueryRequest query, ReplyChannel<QueryResponse> responseHandler);

    /**
     * Handle the given {@code query}, using the given {@code responseHandler} to send the response(s). This operation
     * is flow control aware, hence messages should be sent via {@code responseHandler} when requested.
     * <p>
     * Note that the query <em>must</em> be completed using {@link ReplyChannel#complete()} or {@link
     * ReplyChannel#sendLast(Object)}.
     *
     * @param query           the message representing the query request
     * @param responseHandler the handler to send responses with
     * @return a {@link FlowControl} to request more responses and cancel sending responses
     */
    default FlowControl stream(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
        handle(query, responseHandler);
        return NoopFlowControl.INSTANCE;
    }

    /**
     * Registers an incoming subscription query request, represented by given {@code query}, using given {@code
     * updateHandler} to send updates when the projection for this query changes.
     * <p>
     * If this handler doesn't support subscription queries for the given {@code query}, it should return {@code null}.
     * Otherwise, it must return a handle that can be used to cancel the subscription query.
     *
     * @param query         the message representing the query
     * @param updateHandler to handler to send updates with
     * @return a registration to cancel the subscription, or {@code null} if this handler doesn't support the
     * subscription query
     */
    default Registration registerSubscriptionQuery(SubscriptionQuery query, UpdateHandler updateHandler) {
        return null;
    }

    /**
     * Interface describing a stream of updates to a subscription query.
     */
    interface UpdateHandler {

        /**
         * Send the given {@code queryUpdate} in response to the subscription query this handler was provided for.
         *
         * @param queryUpdate the update to send
         */
        void sendUpdate(QueryUpdate queryUpdate);

        /**
         * Indicates the subscription query has completed, meaning no more updates are to be expected. The component
         * sending the subscription query is requested to close the subscription.
         */
        void complete();
    }
}
