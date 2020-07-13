/*
 * Copyright (c) 2010-2020. Axon Framework
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

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.grpc.FlowControl;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class FlowControlledStream<IN, OUT> implements ClientResponseObserver<OUT, IN> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final AtomicInteger permitsConsumed = new AtomicInteger();
    private final String clientId;
    private final int permits;
    private final int permitsBatch;
    private final FlowControl flowControl;
    private ClientCallStreamObserver<OUT> outboundStream;

    public FlowControlledStream(String clientId, int permits, int permitsBatch) {
        this.clientId = clientId;
        this.permits = permits;
        this.permitsBatch = permitsBatch;
        flowControl = FlowControl.newBuilder()
                                 .setPermits(permitsBatch)
                                 .setClientId(clientId)
                                 .build();
    }

    public void enableFlowControl() {
        if (permitsBatch > 0) {
            permitsConsumed.set(0);
            OUT out = buildInitialFlowControlMessage(FlowControl.newBuilder()
                                                                .setPermits(permits)
                                                                .setClientId(clientId)
                                                                .build());
            if (out != null) {
                outboundStream().onNext(out);
            }
        }
    }

    protected abstract OUT buildFlowControlMessage(FlowControl flowControl);

    protected OUT buildInitialFlowControlMessage(FlowControl flowControl) {
        return buildFlowControlMessage(flowControl);
    }

    protected String clientId() {
        return clientId;
    }

    public void markConsumed() {
        if (permitsBatch > 0) {
            int ticker = permitsConsumed.updateAndGet(current -> {
                if (current == permitsBatch - 1) {
                    return 0;
                }
                return current + 1;
            });
            if (ticker == 0) {
                OUT permitsRequest = buildFlowControlMessage(flowControl);
                if (permitsRequest != null) {
                    logger.debug("Requesting additional {} permits", permitsBatch);
                    outboundStream().onNext(permitsRequest);
                }
            }
        }
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<OUT> requestStream) {
        this.outboundStream = requestStream;
    }

    protected ClientCallStreamObserver<OUT> outboundStream() {
        return outboundStream;
    }
}
