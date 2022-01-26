/*
 * Copyright (c) 2020-2022. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.FlowControl;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.impl.buffer.RoundRobinMultiReadonlyBuffer;
import io.axoniq.axonserver.grpc.ErrorMessage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link FlowControl} that when requested asks a buffer for messages and send them via {@link ReplyChannel}.
 *
 * @param <T> the type of message
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 */
public class FlowControlledReplyChannelWriter<T> implements FlowControl {

    private final DisposableReadonlyBuffer<T> buffer;
    private final ReplyChannel<T> replyChannel;
    private final AtomicLong requestedRef = new AtomicLong();
    private final AtomicBoolean flowGate = new AtomicBoolean();
    private final AtomicBoolean completed = new AtomicBoolean();

    /**
     * Instantiates this flow control with given {@link DisposableReadonlyBuffer}s as sources, and {@link ReplyChannel}
     * as destination. Buffers will be organized in a round-robin fashion (see {@link RoundRobinMultiReadonlyBuffer}).
     *
     * @param sources     {@link DisposableReadonlyBuffer}s to read messages from
     * @param destination {@link ReplyChannel} to write messages to
     */
    public FlowControlledReplyChannelWriter(List<? extends DisposableReadonlyBuffer<T>> sources,
                                            ReplyChannel<T> destination) {
        this(new RoundRobinMultiReadonlyBuffer<>(sources), destination);
    }

    /**
     * Instantiates this flow control with given {@link DisposableReadonlyBuffer} as source, and {@link ReplyChannel}
     * as destination.
     *
     * @param source      {@link DisposableReadonlyBuffer} to read messages from
     * @param destination {@link ReplyChannel} to write messages to
     */
    public FlowControlledReplyChannelWriter(DisposableReadonlyBuffer<T> source, ReplyChannel<T> destination) {
        this.buffer = source;
        this.replyChannel = destination;
        source.onAvailable(this::stream);
    }

    @Override
    public void request(long requested) {
        requestedRef.getAndUpdate(current -> {
            try {
                return Math.addExact(requested, current);
            } catch (ArithmeticException e) {
                return Long.MAX_VALUE;
            }
        });
        stream();
    }

    @Override
    public void cancel() {
        buffer.dispose();
    }

    private void stream() {
        do {
            if (!flowGate.compareAndSet(false, true)) {
                return;
            }
            try {
                long currentPermits = requestedRef.get();
                long sent = send(currentPermits);
                requestedRef.getAndAccumulate(sent, (current, s) -> current - s);
            } finally {
                flowGate.set(false);
            }
        } while (requestedRef.get() > 0 && !buffer.isEmpty());
    }

    private long send(long maxMessages) {
        completeIfBufferIsClosedAndEmpty();
        for (int i = 0; i < maxMessages; i++) {
            Optional<T> element = buffer.poll();
            if (element.isPresent()) {
                replyChannel.send(element.get());
            } else {
                completeIfBufferIsClosedAndEmpty();
                return i;
            }
        }
        return maxMessages;
    }

    private void completeIfBufferIsClosedAndEmpty() {
        if (buffer.closed()
                && buffer.isEmpty()
                && completed.compareAndSet(false, true)) {
            Optional<ErrorMessage> error = buffer.error();
            if (error.isPresent()) {
                replyChannel.completeWithError(error.get());
            } else {
                replyChannel.complete();
            }
        }
    }
}
