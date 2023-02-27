/*
 * Copyright (c) 2020-2023. AxonIQ
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

package io.axoniq.axonserver.connector.event.transformation.impl;

import io.axoniq.axonserver.connector.event.transformation.Transformer;
import io.axoniq.axonserver.connector.event.transformation.impl.EventTransformationService.TransformationStream;
import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class EventTransformer implements Transformer {

    private final AtomicLong sequence;
    private final AtomicLong ackSequence = new AtomicLong(-1L);
    private final TransformationStream transformationStream;
    private final AtomicReference<CompletableFuture<Long>> completeFuture = new AtomicReference<>();

    public EventTransformer(TransformationStream transformationStream,
                            long currentSequence) {
        this.transformationStream = transformationStream;
        this.sequence = new AtomicLong(currentSequence);
    }

    @Override
    public CompletableFuture<Transformer> deleteEvent(long token) {
        long seq = sequence.incrementAndGet();
        return transformationStream().deleteEvent(token, seq)
                                     .thenRun(() -> acceptAck(seq))
                                     .thenApply(unused -> this);
    }


    @Override
    public CompletableFuture<Transformer> replaceEvent(long token, Event replacement) {
        long seq = sequence.incrementAndGet();
        return transformationStream().replaceEvent(token, replacement, seq)
                                     .thenRun(() -> acceptAck(seq))
                                     .thenApply(unused -> this);
    }

    public CompletableFuture<Long> complete() {
        completeFuture.compareAndSet(null, new CompletableFuture<>());
        checkComplete();
        return completeFuture.get()
                             .thenApply(seq -> {
                                 transformationStream.complete();
                                 return seq;
                             });
    }

    private TransformationStream transformationStream() {
        return transformationStream;
    }

    private void acceptAck(long ackSeq) {
        ackSequence.updateAndGet(current -> Math.max(current, ackSeq));
        checkComplete();
    }

    private void checkComplete() {
        CompletableFuture<Long> completed = completeFuture.get();
        if (completed != null && sequence.get() == ackSequence.get()) {
            completed.complete(sequence.get());
        }
    }
}
