/*
 * Copyright (c) 2020-2024. AxonIQ
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

import io.axoniq.axonserver.connector.event.PersistentStreamSegment;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.streams.Requests;
import io.axoniq.axonserver.grpc.streams.StreamRequest;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.LongConsumer;

/**
 * Implementation of the {@link PersistentStreamSegment}.
 */
public class BufferedPersistentStreamSegment
        extends AbstractBufferedStream<EventWithToken, StreamRequest>
        implements PersistentStreamSegment {

    private static final EventWithToken TERMINAL_MESSAGE = EventWithToken.newBuilder().setToken(-1729).build();

    private final Set<Runnable> onSegmentClosedCallbacks = new CopyOnWriteArraySet<>();

    private final int segment;
    private final LongConsumer progressCallback;

    /**
     * Constructs a {@link BufferedPersistentStreamSegment}.
     * @param segment          the index of the segment
     * @param bufferSize       the number of events to buffer locally
     * @param refillBatch      the number of events to be consumed prior to refilling the buffer
     * @param progressCallback the callback to invoke for acknowledging processed events
     */
    public BufferedPersistentStreamSegment(int segment,
                                           int bufferSize,
                                           int refillBatch,
                                           LongConsumer progressCallback) {
        super("ignoredClientId", bufferSize, refillBatch);
        this.segment = segment;
        this.progressCallback = progressCallback;
    }

    @Override
    public void onSegmentClosed(Runnable callback) {
        onSegmentClosedCallbacks.add(callback);
    }

    @Override
    public void onCompleted() {
        super.onCompleted();
        onSegmentClosedCallbacks.forEach(Runnable::run);
    }

    @Override
    public void acknowledge(long token) {
        if (!isClosed()) {
            progressCallback.accept(token);
        }
    }

    @Override
    public int segment() {
        return segment;
    }


    @Override
    protected EventWithToken terminalMessage() {
        return TERMINAL_MESSAGE;
    }

    @Override
    protected StreamRequest buildFlowControlMessage(FlowControl flowControl) {
        return StreamRequest.newBuilder()
                            .setRequests(Requests.newBuilder()
                                                 .setSegment(segment)
                                                 .setRequests((int) flowControl.getPermits()))

                            .build();
    }
}
