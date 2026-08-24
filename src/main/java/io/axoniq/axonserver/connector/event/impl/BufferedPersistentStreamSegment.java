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
import io.axoniq.axonserver.grpc.streams.PersistentStreamEvent;
import io.axoniq.axonserver.grpc.streams.Requests;
import io.axoniq.axonserver.grpc.streams.StreamRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Implementation of the {@link PersistentStreamSegment}.
 */
public class BufferedPersistentStreamSegment
        extends AbstractBufferedStream<PersistentStreamEvent, StreamRequest>
        implements PersistentStreamSegment {

    private static final Logger logger = LoggerFactory.getLogger(BufferedPersistentStreamSegment.class);
    private static final PersistentStreamEvent TERMINAL_MESSAGE = PersistentStreamEvent.newBuilder().setEvent(
            EventWithToken.newBuilder().setToken(-1729).build()).build();

    private final Set<Runnable> onSegmentClosedCallbacks = new CopyOnWriteArraySet<>();

    private final String streamId;
    private final int segment;
    private final LongConsumer progressCallback;
    private final Consumer<String> errorCallback;
    /**
     * Guards {@link #onCompleted()}/{@link #close()} so their completion effect (enqueueing the terminal message,
     * notifying {@link #onSegmentClosed(Runnable) segment-closed} listeners) runs exactly once, regardless of which of
     * the two triggers it first. Deliberately NOT used to back {@link #isClosed()}. That must reflect whether the
     * buffer has actually been drained, not merely whether a close/complete signal has been observed, otherwise
     * already-buffered events become silently unreachable.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Constructs a {@link BufferedPersistentStreamSegment}.
     *
     * @param streamId         the id of the persistent stream
     * @param segment          the index of the segment
     * @param bufferSize       the number of events to buffer locally
     * @param refillBatch      the number of events to be consumed prior to refilling the buffer
     * @param progressCallback the callback to invoke for acknowledging processed events
     */
    public BufferedPersistentStreamSegment(String streamId,
                                           int segment,
                                           int bufferSize,
                                           int refillBatch,
                                           LongConsumer progressCallback,
                                           Consumer<String> errorCallback) {
        super("ignoredClientId", bufferSize, refillBatch);
        this.streamId = streamId;
        this.segment = segment;
        this.progressCallback = progressCallback;
        this.errorCallback = errorCallback;
    }

    @Override
    public void onSegmentClosed(Runnable callback) {
        onSegmentClosedCallbacks.add(callback);
    }

    @Override
    public void onCompleted() {
        if (closed.compareAndSet(false, true)) {
            super.onCompleted();
            onSegmentClosedCallbacks.forEach(Runnable::run);
        }
    }

    @Override
    public void acknowledge(long token) {
        if (closed.get()) {
            logger.debug("{}: Acknowledging position {} for segment {} after closing the segment",
                         streamId, token, segment);
        }
        progressCallback.accept(token);
    }

    @Override
    public void error(String error) {
        errorCallback.accept(error);
    }

    @Override
    public int segment() {
        return segment;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("{}: Close segment {}", streamId, segment);
            super.onCompleted();
            onSegmentClosedCallbacks.forEach(Runnable::run);
        }
    }

    @Override
    protected PersistentStreamEvent terminalMessage() {
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

    @Override
    public String toString() {
        return streamId + "[" + segment + "]";
    }
}
