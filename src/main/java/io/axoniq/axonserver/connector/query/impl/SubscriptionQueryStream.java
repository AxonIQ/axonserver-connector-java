package io.axoniq.axonserver.connector.query.impl;

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCode;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.connector.impl.FlowControlledStream;
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

public class SubscriptionQueryStream extends FlowControlledStream<SubscriptionQueryResponse, SubscriptionQueryRequest> {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionQueryStream.class);

    private final String subscriptionQueryId;
    private final CompletableFuture<QueryResponse> initialResultFuture;
    private final AbstractBufferedStream<QueryUpdate, SubscriptionQueryRequest> updateBuffer;

    public SubscriptionQueryStream(String subscriptionQueryId, CompletableFuture<QueryResponse> initialResultFuture, String clientId, int bufferSize, int fetchSize) {
        super(clientId, bufferSize, fetchSize);
        this.subscriptionQueryId = subscriptionQueryId;
        this.initialResultFuture = initialResultFuture;
        this.updateBuffer = new SubscriptionQueryUpdateBuffer(clientId, subscriptionQueryId, bufferSize, fetchSize);
    }

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
                AxonServerException exception = new AxonServerException(ErrorCode.getFromCode(value.getCompleteExceptionally().getErrorCode()),
                                                                        value.getCompleteExceptionally().getErrorMessage().getMessage());
                updateBuffer.onError(exception);
                if (!initialResultFuture.isDone()) {
                    initialResultFuture.completeExceptionally(exception);
                }
                break;
            case INITIAL_RESULT:
                initialResultFuture.complete(value.getInitialResult());
                break;
        }
    }

    @Override
    public void onError(Throwable t) {
        initialResultFuture.completeExceptionally(t);
        updateBuffer.onError(t);

        try {
            outboundStream().onNext(SubscriptionQueryRequest.newBuilder().setUnsubscribe(SubscriptionQuery.newBuilder().setSubscriptionIdentifier(subscriptionQueryId).build()).build());
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
        super.beforeStart(requestStream);
        updateBuffer.beforeStart(requestStream);
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
