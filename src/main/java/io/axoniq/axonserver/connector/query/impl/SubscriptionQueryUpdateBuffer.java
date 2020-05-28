package io.axoniq.axonserver.connector.query.impl;

import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;

import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriptionQueryUpdateBuffer extends AbstractBufferedStream<QueryUpdate, SubscriptionQueryRequest> {

    private static final QueryUpdate TERMINAL_MESSAGE = QueryUpdate.newBuilder().setClientId("__terminal__").build();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final String subscriptionQueryId;
    private final int refillBatch;
    private final SubscriptionQueryRequest refillRequest;

    public SubscriptionQueryUpdateBuffer(String clientId, String subscriptionQueryId, int bufferSize, int refillBatch) {
        super(clientId, bufferSize, refillBatch);
        this.subscriptionQueryId = subscriptionQueryId;
        this.refillBatch = refillBatch;
        this.refillRequest = SubscriptionQueryRequest.newBuilder().setFlowControl(SubscriptionQuery.newBuilder().setNumberOfPermits(refillBatch).build()).build();
    }

    @Override
    protected QueryUpdate terminalMessage() {
        return TERMINAL_MESSAGE;
    }

    @Override
    protected SubscriptionQueryRequest buildFlowControlMessage(FlowControl flowControl) {
        if (refillBatch == flowControl.getPermits()) {
            return refillRequest;
        }
        return SubscriptionQueryRequest.newBuilder().setFlowControl(SubscriptionQuery.newBuilder().setNumberOfPermits(flowControl.getPermits()).build()).build();
    }

    @Override
    public void onNext(QueryUpdate value) {
        // ignore all messages after closing
        if (!closed.get()) {
            super.onNext(value);
        }
    }

    @Override
    public void onError(Throwable t) {
        try {
            super.onError(t);
        } finally {
            close();
        }
    }

    @Override
    public void onCompleted() {
        try {
            super.onCompleted();
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            outboundStream().onNext(SubscriptionQueryRequest.newBuilder().setUnsubscribe(SubscriptionQuery.newBuilder().setSubscriptionIdentifier(subscriptionQueryId).build()).build());
            // complete the buffer
            onCompleted();
            // complete the stream to the server
            outboundStream().onCompleted();
        }
    }
}
