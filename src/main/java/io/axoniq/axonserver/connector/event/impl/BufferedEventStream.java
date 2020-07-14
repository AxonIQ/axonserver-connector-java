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

package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.PayloadDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.nonNullOrDefault;

public class BufferedEventStream extends AbstractBufferedStream<EventWithToken, GetEventsRequest> implements EventStream {

    private static final Logger logger = LoggerFactory.getLogger(BufferedEventStream.class);
    private static final EventWithToken TERMINAL_MESSAGE = EventWithToken.newBuilder().setToken(-1729).build();

    private final long trackingToken;
    private final boolean forceReadFromLeader;

    public BufferedEventStream(long trackingToken, int bufferSize, int refillBatch, boolean forceReadFromLeader) {
        super("unused", bufferSize, refillBatch);
        this.trackingToken = trackingToken;
        this.forceReadFromLeader = forceReadFromLeader;
    }

    @Override
    protected GetEventsRequest buildFlowControlMessage(FlowControl flowControl) {
        GetEventsRequest request = GetEventsRequest.newBuilder().setNumberOfPermits(flowControl.getPermits()).build();
        logger.trace("Sending request for data: {}", request);
        return request;
    }

    @Override
    protected GetEventsRequest buildInitialFlowControlMessage(FlowControl flowControl) {
        GetEventsRequest eventsRequest = GetEventsRequest.newBuilder()
                                                         .setTrackingToken(trackingToken + 1)
                                                         .setForceReadFromLeader(forceReadFromLeader)
                                                         .setNumberOfPermits(flowControl.getPermits()).build();
        logger.trace("Sending request for data: {}", eventsRequest);
        return eventsRequest;
    }

    @Override
    protected EventWithToken terminalMessage() {
        return TERMINAL_MESSAGE;
    }

    @Override
    public void excludePayloadType(String payloadType, String revision) {
        GetEventsRequest request = GetEventsRequest.newBuilder()
                                                   .addBlacklist(PayloadDescription.newBuilder()
                                                                                   .setType(payloadType)
                                                                                   .setRevision(nonNullOrDefault(revision, ""))
                                                                                   .build())
                                                   .build();
        logger.trace("Requesting exclusion of message type: {}", request);
        outboundStream().onNext(request);
    }
}
