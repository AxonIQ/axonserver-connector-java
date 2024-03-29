/*
 * Copyright (c) 2021. AxonIQ
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
import io.axoniq.axonserver.connector.impl.StreamTimeoutException;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;

import java.util.concurrent.TimeUnit;

/**
 * Buffering implementation of the {@link AggregateEventStream} used for Event Sourced Aggregates.
 */
public class BufferedAggregateEventStream
        extends FlowControlledBuffer<Event, GetAggregateEventsRequest>
        implements AggregateEventStream {

    private static final Event TERMINAL_MESSAGE = Event.newBuilder().setAggregateSequenceNumber(-1729).build();
    private static final int TAKE_TIMEOUT_MILLIS = Integer.parseInt(
            System.getProperty("AGGREGATE_TAKE_EVENT_TIMEOUT_MILLIS", "10000")
    );

    private Event peeked;
    private long lastSequenceNumber = -1;

    /**
     * Constructs a {@link BufferedAggregateEventStream} with a buffer size of {@code 512} and a {@code refillBatch} of
     * 16.
     */
    public BufferedAggregateEventStream() {
        this(512, 16);
    }

    /**
     * Constructs a {@link BufferedAggregateEventStream} with the given {@code bufferSize}.
     *
     * @param bufferSize the buffer size of the aggregate event stream
     */
    public BufferedAggregateEventStream(int bufferSize, int refillBatch) {
        super("unused", bufferSize, refillBatch);
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
        if (taken != null) {
            lastSequenceNumber = taken.getAggregateSequenceNumber();
        }
        return taken;
    }

    @Override
    public boolean hasNext() {
        if (peeked != null) {
            return true;
        }
        try {
            peeked = tryTake(TAKE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS, true);
        } catch (InterruptedException e) {
            cancel();
            Thread.currentThread().interrupt();
            return false;
        } catch (StreamTimeoutException e) {
            throw new RuntimeException(String.format(
                    "Was unable to load aggregate due to timeout while waiting for events. Last sequence number received: %d",
                    lastSequenceNumber
            ), e);
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
        // no app-level flow control available on this request
        return null;
    }

    @Override
    protected Event terminalMessage() {
        return TERMINAL_MESSAGE;
    }

}
