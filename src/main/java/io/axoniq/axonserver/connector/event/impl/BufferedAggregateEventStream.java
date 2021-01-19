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

package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.event.AggregateEventStream;
import io.axoniq.axonserver.connector.impl.FlowControlledBuffer;
import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Buffering implementation of the {@link AggregateEventStream} used for Event Sourced Aggregates.
 */
public class BufferedAggregateEventStream
        extends FlowControlledBuffer<Event, GetAggregateEventsRequest>
        implements AggregateEventStream {

    private static final Logger logger = LoggerFactory.getLogger(BufferedAggregateEventStream.class);

    private static final Event TERMINAL_MESSAGE = Event.newBuilder().setAggregateSequenceNumber(-1729).build();
    private final AtomicReference<Long> lastReceivedEventSequence = new AtomicReference<>();

    private Event peeked;

    /**
     * Constructs a {@link BufferedAggregateEventStream} with a buffer size of {@link Integer#MAX_VALUE}.
     */
    public BufferedAggregateEventStream() {
        this(Integer.MAX_VALUE);
    }

    /**
     * Constructs a {@link BufferedAggregateEventStream} with the given {@code bufferSize}.
     *
     * @param bufferSize the buffer size of the aggregate event stream
     */
    public BufferedAggregateEventStream(int bufferSize) {
        super("unused", bufferSize, 0);
    }

    @Override
    public Event next() throws InterruptedException {
        Event taken;
        if (peeked != null) {
            taken = peeked;
            peeked = null;
        } else {
            taken = take();
        }
        return taken;
    }

    @Override
    public boolean hasNext() {
        if (peeked != null) {
            return true;
        }
        try {
            peeked = tryTake();
        } catch (InterruptedException e) {
            cancel();
            Thread.currentThread().interrupt();
            return false;
        }
        if (peeked == null) {
            Throwable errorResult = getErrorResult();
            if (errorResult != null) {
                throw new StreamClosedException(errorResult);
            }
        }
        return peeked != null;
    }

    @Override
    public void cancel() {
        outboundStream().cancel("Request cancelled by client", null);
    }

    @Override
    protected GetAggregateEventsRequest buildFlowControlMessage(FlowControl flowControl) {
        // no flow control available on this request
        return null;
    }

    @Override
    protected Event terminalMessage() {
        return TERMINAL_MESSAGE;
    }

    @Override
    public void onNext(Event event) {
        Long prevSequence = lastReceivedEventSequence.get();
        if (prevSequence == null || prevSequence + 1L == event.getAggregateSequenceNumber()) {
            super.onNext(event);
            lastReceivedEventSequence.set(event.getAggregateSequenceNumber());
        } else {
            String message = String.format(
                    "Invalid sequence number for aggregate with identifier [%s]. Received seqNo: %d, expected seqNo: %d",
                    event.getAggregateIdentifier(),
                    event.getAggregateSequenceNumber(),
                    prevSequence + 1L);
            logger.error(message);
            RuntimeException invalidAggregateEventStreamException = new RuntimeException(message);
            StreamObserver<GetAggregateEventsRequest> outboundStream = outboundStream();
            if (outboundStream != null) {
                outboundStream.onError(invalidAggregateEventStreamException);
            }
            this.onError(invalidAggregateEventStreamException);
        }
    }
}
