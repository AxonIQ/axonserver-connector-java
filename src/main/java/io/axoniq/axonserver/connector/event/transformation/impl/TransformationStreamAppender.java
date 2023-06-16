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

import io.axoniq.axonserver.connector.event.transformation.Appender;
import io.axoniq.axonserver.connector.event.transformation.impl.EventTransformationService.TransformationStream;
import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link Appender} implementation that uses the {@link TransformationStream} to interact with Axon Server
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public class TransformationStreamAppender implements Appender {

    private final AtomicLong sequence;
    private final AtomicLong ackSequence = new AtomicLong(-1L);
    private final TransformationStream transformationStream;
    private final AtomicReference<CompletableFuture<Long>> completeFuture = new AtomicReference<>();

    /**
     * Constructs a {@link TransformationStreamAppender} based on the specified parameters.
     *
     * @param transformationStream the {@link TransformationStream} used to communicate with Axon Server
     * @param currentSequence      the current last sequence, used as a validation that no transformation action has
     *                             been lost
     */
    TransformationStreamAppender(TransformationStream transformationStream, long currentSequence) {
        this.transformationStream = transformationStream;
        this.sequence = new AtomicLong(currentSequence);
        this.transformationStream.onCompletedByServer(this::completedByServer);
    }

    @Override
    public CompletableFuture<Appender> deleteEvent(long token) {
        checkCompleted();
        long seq = sequence.incrementAndGet();
        return transformationStream().deleteEvent(token, seq).thenRun(() -> acceptAck(seq)).thenApply(unused -> this);
    }


    @Override
    public CompletableFuture<Appender> replaceEvent(long token, Event replacement) {
        checkCompleted();
        long seq = sequence.incrementAndGet();
        return transformationStream().replaceEvent(token, replacement, seq)
                                     .thenRun(() -> acceptAck(seq))
                                     .thenApply(unused -> this);
    }

    private void checkCompleted() {
        CompletableFuture<Long> completed = completeFuture.get();
        if (completed == null) {
            return;
        }
        if (completed.isDone()) {
            throw new TransformationStreamCompletedException();
        }
        if (completed.isCompletedExceptionally()) {
            completed.join();
        }
    }

    /**
     * Returns a {@link CompletableFuture} that completes when all the change requests have been appended. The
     * {@link CompletableFuture} will contain the last sequence of the transformation after last request has been
     * appended.
     *
     * @return a {@link CompletableFuture} that completes when all the change requests have been appended, containing
     * the updated last sequence of the event transformation.
     */
    public CompletableFuture<Long> complete() {
        completeFuture.compareAndSet(null, new CompletableFuture<Long>()
                .thenApply(seq -> {
                    transformationStream.complete();
                    return seq;
                }));
        checkComplete();
        return completeFuture.get();
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
        if (completed == null) {
            return;
        }
        if (sequence.get() == ackSequence.get()) {
            completed.complete(sequence.get());
        }
    }

    private void completedByServer(Throwable error) {
        completeFuture.compareAndSet(null, new CompletableFuture<>());
        completeFuture.get().completeExceptionally(error);
    }
}
