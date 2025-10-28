/*
 * Copyright (c) 2020-2021. AxonIQ
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

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * A {@link FlowControlledStream} implementation to return the results of a {@link SubscriptionQuery}.
 */
public class SubscriptionQueryStream extends FlowControlledStream<SubscriptionQueryResponse, SubscriptionQueryRequest> {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionQueryStream.class);

    private final String subscriptionQueryId;
    private final AbstractBufferedStream<QueryUpdate, SubscriptionQueryRequest> updateBuffer;
    private final Supplier<ResultStream<QueryResponse>> initialResultSupplier;

    private final AtomicReference<ResultStream<QueryResponse>> initialResult = new AtomicReference<>();

    /**
     * Instantiates a {@link SubscriptionQueryStream} to stream {@link SubscriptionQuery} results.
     *
     * @param subscriptionQueryId   the identifier of this subscription query
     * @param clientId              the identifier of the client initiating the subscription query
     * @param initialResultSupplier a {@link Supplier} providing the initial {@link ResultStream} of the subscription query
     * @param bufferSize            the size of the update {@link #updatesBuffer()}
     * @param fetchSize             the number of updates to be consumed prior to refilling the update
     *                              {@link #updatesBuffer()}
     */
    public SubscriptionQueryStream(String subscriptionQueryId,
                                   String clientId,
                                   Supplier<ResultStream<QueryResponse>> initialResultSupplier,
                                   int bufferSize,
                                   int fetchSize) {
        super(clientId, bufferSize, fetchSize);
        this.initialResultSupplier = initialResultSupplier;
        this.subscriptionQueryId = subscriptionQueryId;
        this.updateBuffer = new SubscriptionQueryUpdateBuffer(clientId, subscriptionQueryId, bufferSize, fetchSize);
    }

    /**
     * Returns the {@link ResultStream} buffering the {@link QueryUpdate}s to this subscription query.
     *
     * @return the {@link ResultStream} buffering the {@link QueryUpdate}s to this subscription query
     */
    public ResultStream<QueryUpdate> updatesBuffer() {
        return updateBuffer;
    }

    /**
     *
     * Returns the {@link ResultStream} buffering the initial results to this subscription query.
     *
     * @return the {@link ResultStream} buffering the initial results to this subscription query
     */
    public ResultStream<QueryResponse> initialStream() {
        return initialResult.updateAndGet(i -> i == null ? buildInitialResultStream() : i);
    }

    private ResultStream<QueryResponse> buildInitialResultStream() {
        ResultStream<QueryResponse> delegate = initialResultSupplier.get();
        delegate.onAvailable(() -> {
            if (delegate.getError().isPresent()) {
                // close the update stream, since initial results completed with an error
                updatesBuffer().close();
            }
        });
        return new ResultStream<>() {
            @Override
            public QueryResponse peek() {
                return delegate.peek();
            }

            @Override
            public QueryResponse nextIfAvailable() {
                return delegate.nextIfAvailable();
            }

            @Override
            public QueryResponse nextIfAvailable(long timeout, TimeUnit unit) throws InterruptedException {
                return delegate.nextIfAvailable(timeout, unit);
            }

            @Override
            public QueryResponse next() throws InterruptedException {
                return delegate.next();
            }

            @Override
            public void onAvailable(Runnable callback) {
                delegate.onAvailable(() -> {
                    if (delegate.getError().isPresent()) {
                        // close the update stream, since initial results completed with an error
                        updatesBuffer().close();
                    }
                    callback.run();
                });
            }

            @Override
            public void close() {
                delegate.close();
                SubscriptionQueryStream.this.updatesBuffer().close();
            }

            @Override
            public boolean isClosed() {
                return delegate.isClosed();
            }

            @Override
            public Optional<Throwable> getError() {
                return delegate.getError();
            }
        };
    }

    @Override
    public void onNext(SubscriptionQueryResponse value) {
        switch (value.getResponseCase()) {
            case UPDATE:
                logger.debug("Received subscription query update. Subscription Id: {}. Message Id: {}.",
                             value.getSubscriptionIdentifier(),
                             value.getMessageIdentifier());
                updateBuffer.onNext(value.getUpdate());
                break;
            case COMPLETE:
                logger.debug("Received subscription query complete. Subscription Id: {}.",
                             value.getSubscriptionIdentifier());
                updateBuffer.onCompleted();
                break;
            case COMPLETE_EXCEPTIONALLY:
                logger.debug("Received subscription query complete exceptionally. Subscription Id: {}.",
                             value.getSubscriptionIdentifier());
                AxonServerException exception = new AxonServerException(
                        ErrorCategory.getFromCode(value.getCompleteExceptionally().getErrorCode()),
                        value.getCompleteExceptionally().getErrorMessage().getMessage(),
                        value.getCompleteExceptionally().getClientId()
                );
                updateBuffer.onError(exception);
                ResultStream<QueryResponse> initial = initialResult.get();
                if (initial != null && !initial.isClosed()) {
                    initial.close();
                }
                break;
            case INITIAL_RESULT:
            default:
                logger.info("Received unsupported message from SubscriptionQuery. "
                                    + "It doesn't declare one of the expected types: {}",
                            value.getResponseCase());
                break;
        }
    }

    @Override
    public void onError(Throwable t) {
        ResultStream<QueryResponse> initialResult = this.initialResult.get();
        if (initialResult != null && !initialResult.isClosed()) {
            initialResult.close();
        }
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
