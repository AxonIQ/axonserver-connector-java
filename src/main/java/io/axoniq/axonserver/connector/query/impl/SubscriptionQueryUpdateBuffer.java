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

import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Buffer for {@link SubscriptionQuery} updates.
 */
public class SubscriptionQueryUpdateBuffer extends AbstractBufferedStream<QueryUpdate, SubscriptionQueryRequest> {

    private static final QueryUpdate TERMINAL_MESSAGE = QueryUpdate.newBuilder().setClientId("__terminal__").build();

    private final AtomicBoolean closed = new AtomicBoolean();
    private final String subscriptionQueryId;
    private final int refillBatch;
    private final SubscriptionQueryRequest refillRequest;

    /**
     * Instantiates a {@link SubscriptionQueryUpdateBuffer}.
     *
     * @param clientId            the identifier of the client initiating the subscription query
     * @param subscriptionQueryId the identifier of the subscription query this buffer buffers for
     * @param bufferSize          the size of this buffer
     * @param refillBatch         the number of updates to be consumed prior to refilling this buffer
     */
    public SubscriptionQueryUpdateBuffer(String clientId, String subscriptionQueryId, int bufferSize, int refillBatch) {
        super(clientId, bufferSize, refillBatch);
        this.subscriptionQueryId = subscriptionQueryId;
        this.refillBatch = refillBatch;
        this.refillRequest = SubscriptionQueryRequest.newBuilder()
                                                     .setFlowControl(SubscriptionQuery.newBuilder()
                                                                                      .setNumberOfPermits(refillBatch)
                                                                                      .build())
                                                     .build();
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
        return SubscriptionQueryRequest.newBuilder()
                                       .setFlowControl(SubscriptionQuery.newBuilder()
                                                                        .setNumberOfPermits(flowControl.getPermits())
                                                                        .build())
                                       .build();
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
            SubscriptionQuery subscriptionQueryToUnsubscribe =
                    SubscriptionQuery.newBuilder().setSubscriptionIdentifier(subscriptionQueryId).build();
            outboundStream().onNext(SubscriptionQueryRequest.newBuilder()
                                                            .setUnsubscribe(subscriptionQueryToUnsubscribe)
                                                            .build());
            // complete the buffer
            onCompleted();
            // complete the stream to the server
            outboundStream().onCompleted();
        }
    }
}
