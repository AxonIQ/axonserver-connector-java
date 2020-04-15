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
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class FlowControlledStream<MsgIn, MsgOut> implements ClientResponseObserver<MsgOut, MsgIn> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final AtomicInteger permitsConsumed = new AtomicInteger();
    private final int permitsBatch;
    private final FlowControl additionalPermitsRequest;
    private final FlowControl initialPermitsRequest;

    public FlowControlledStream(String clientId, int permits, int permitsBatch) {
        this.permitsBatch = permitsBatch;
        this.additionalPermitsRequest = FlowControl.newBuilder()
                                                   .setPermits(permitsBatch)
                                                   .setClientId(clientId)
                                                   .build();
        this.initialPermitsRequest = FlowControl.newBuilder()
                                                .setPermits(permits)
                                                .setClientId(clientId)
                                                .build();
    }

    public void enableFlowControl() {
        permitsConsumed.set(0);
        outboundStream().onNext(buildFlowControlMessage(initialPermitsRequest));
    }

    protected abstract MsgOut buildFlowControlMessage(FlowControl flowControl);

    public void markConsumed() {
        int ticker = permitsConsumed.updateAndGet(current -> {
            if (current == permitsBatch - 1) {
                return 0;
            }
            return current + 1;
        });
        if (ticker == 0) {
            logger.info("Requesting additional permits");
            outboundStream().onNext(buildFlowControlMessage(additionalPermitsRequest));
        }
    }

    protected abstract StreamObserver<MsgOut> outboundStream();
}
