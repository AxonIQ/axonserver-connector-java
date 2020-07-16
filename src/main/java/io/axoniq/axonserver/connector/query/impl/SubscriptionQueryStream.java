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

package io.axoniq.axonserver.connector.query.impl;

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.connector.impl.FlowControlledStream;
import io.axoniq.axonserver.connector.impl.SynchronizedRequestStream;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.grpc.stub.ClientCallStreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * A {@link FlowControlledStream} implementation to return the results of a {@link SubscriptionQuery}.
 */
public class SubscriptionQueryStream extends FlowControlledStream<SubscriptionQueryResponse, SubscriptionQueryRequest> {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionQueryStream.class);

    private final String subscriptionQueryId;
    private final CompletableFuture<QueryResponse> initialResultFuture;
    private final AbstractBufferedStream<QueryUpdate, SubscriptionQueryRequest> updateBuffer;

    /**
     * Instantiates a {@link SubscriptionQueryStream} to stream {@link SubscriptionQuery} results.
     *
     * @param subscriptionQueryId the identifier of this subscription query
     * @param initialResultFuture the initial result of the subscription query
     * @param clientId            the identifier of the client initiating the subscription query
     * @param bufferSize          the size of the update {@link #buffer()}
     * @param fetchSize           the number of updates to be consumed prior to refilling the update {@link #buffer()}
     */
    public SubscriptionQueryStream(String subscriptionQueryId,
                                   CompletableFuture<QueryResponse> initialResultFuture,
                                   String clientId,
                                   int bufferSize,
                                   int fetchSize) {
        super(clientId, bufferSize, fetchSize);
        this.subscriptionQueryId = subscriptionQueryId;
        this.initialResultFuture = initialResultFuture;
        this.updateBuffer = new SubscriptionQueryUpdateBuffer(clientId, subscriptionQueryId, bufferSize, fetchSize);
    }

    /**
     * Returns the {@link ResultStream} buffering the {@link QueryUpdate}s to this subscription query.
     *
     * @return the {@link ResultStream} buffering the {@link QueryUpdate}s to this subscription query
     */
    public ResultStream<QueryUpdate> buffer() {
        return updateBuffer;
    }

    @Override
    public void onNext(SubscriptionQueryResponse value) {
        switch (value.getResponseCase()) {
            case UPDATE:
                updateBuffer.onNext(value.getUpdate());
                break;
            case COMPLETE:
                updateBuffer.onCompleted();
                break;
            case COMPLETE_EXCEPTIONALLY:
                AxonServerException exception = new AxonServerException(
                        ErrorCategory.getFromCode(value.getCompleteExceptionally().getErrorCode()),
                        value.getCompleteExceptionally().getErrorMessage().getMessage(),
                        value.getCompleteExceptionally().getClientId()
                );
                updateBuffer.onError(exception);
                if (!initialResultFuture.isDone()) {
                    initialResultFuture.completeExceptionally(exception);
                }
                break;
            case INITIAL_RESULT:
                initialResultFuture.complete(value.getInitialResult());
                break;
            default:
                logger.info("Received unsupported message from SubscriptionQuery. "
                                    + "It doesn't declare one of the expected types");
                break;
        }
    }

    @Override
    public void onError(Throwable t) {
        initialResultFuture.completeExceptionally(t);
        updateBuffer.onError(t);

        try {
            SubscriptionQuery subscriptionQueryToUnsubscribe =
                    SubscriptionQuery.newBuilder().setSubscriptionIdentifier(subscriptionQueryId).build();
            outboundStream().onNext(SubscriptionQueryRequest.newBuilder()
                                                            .setUnsubscribe(subscriptionQueryToUnsubscribe)
                                                            .build());
            outboundStream().onCompleted();
        } catch (Exception e) {
            logger.debug("Cannot complete stream. Already completed.", e);
        }
    }

    @Override
    public void onCompleted() {
        updateBuffer.onCompleted();
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<SubscriptionQueryRequest> requestStream) {
        SynchronizedRequestStream<SubscriptionQueryRequest> synchronizedRequestStream =
                new SynchronizedRequestStream<>(requestStream);
        super.beforeStart(synchronizedRequestStream);
        updateBuffer.beforeStart(synchronizedRequestStream);
    }

    @Override
    public void enableFlowControl() {
        updateBuffer.enableFlowControl();
    }

    @Override
    protected SubscriptionQueryRequest buildFlowControlMessage(FlowControl flowControl) {
        // we manage flow control through the buffer
        return null;
    }
}
