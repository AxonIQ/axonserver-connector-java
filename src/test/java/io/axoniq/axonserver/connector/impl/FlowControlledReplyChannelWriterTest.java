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
import io.axoniq.axonserver.connector.impl.buffer.BlockingCloseableBuffer;
import io.axoniq.axonserver.connector.impl.buffer.FlowControlledDisposableReadonlyBuffer;
import io.axoniq.axonserver.grpc.ErrorMessage;
import org.junit.jupiter.api.*;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static org.mockito.Mockito.*;

/**
 * Tests for the {@link FlowControlledReplyChannelWriter}.
 */
class FlowControlledReplyChannelWriterTest {

    private final AtomicReference<Runnable> onAvailableRef = new AtomicReference<>();
    private DisposableReadonlyBuffer<String> source;
    private ReplyChannel<String> destination;
    private FlowControlledReplyChannelWriter<String> writer;

    @BeforeEach
    void setUp() {
        source = mock(DisposableReadonlyBuffer.class);
        doAnswer(invocation -> {
            Runnable onAvailable = invocation.getArgument(0, Runnable.class);
            onAvailableRef.set(onAvailable);
            return null;
        }).when(source).onAvailable(any(Runnable.class));
        destination = mock(ReplyChannel.class);
        writer = new FlowControlledReplyChannelWriter<>(source, destination);
    }

    @Test
    void testRequestWhenThereAreElementsInBuffer() {
        when(source.poll()).thenReturn(Optional.of("element"));

        writer.request(10);

        verify(destination, times(10)).send("element");
    }

    @Test
    void testRequestWhenElementsBecomeAvailableLater() throws InterruptedException, ExecutionException,
                                                              TimeoutException {
        when(source.poll()).thenReturn(Optional.empty());
        when(source.closed()).thenReturn(false);
        when(source.isEmpty()).thenReturn(true);

        writer.request(5);

        ScheduledFuture<?> schedule = Executors.newSingleThreadScheduledExecutor()
                                               .schedule(() -> {
                                                   when(source.isEmpty()).thenReturn(false);
                                                   when(source.poll()).thenReturn(Optional.of("element"));
                                                   onAvailableRef.get().run();
                                               }, 100, TimeUnit.MILLISECONDS);
        schedule.get(1, TimeUnit.SECONDS);

        verify(destination, times(5)).send("element");
    }

    @Test
    void testRequestWhenSourceCompletedButStillHasElements() {
        BlockingCloseableBuffer<String> buffer = new BlockingCloseableBuffer<>();
        FlowControl flowControl = new FlowControl() {
            @Override
            public void request(long requested) {
                LongStream.rangeClosed(1, 3)
                          .forEach(l -> buffer.put("v" + l));
                buffer.close();
            }

            @Override
            public void cancel() {
                // noop
            }
        };
        source = new FlowControlledDisposableReadonlyBuffer<>(flowControl, buffer);
        writer = new FlowControlledReplyChannelWriter<>(source, destination);

        writer.request(5);

        verify(destination).send("v1");
        verify(destination).send("v2");
        verify(destination).send("v3");
        verify(destination).complete();
    }

    @Test
    void testBufferCompletionWithoutRequest() {
        BlockingCloseableBuffer<String> buffer = new BlockingCloseableBuffer<>();
        FlowControl flowControl = new FlowControl() {
            @Override
            public void request(long requested) {
                LongStream.rangeClosed(1, 3)
                          .forEach(l -> buffer.put("v" + l));
                buffer.close();
            }

            @Override
            public void cancel() {
                // noop
            }
        };
        source = new FlowControlledDisposableReadonlyBuffer<>(flowControl, buffer);
        writer = new FlowControlledReplyChannelWriter<>(source, destination);

        buffer.close();

        verify(destination).complete();
    }

    @Test
    void testRequestWhenSourceErrored() {
        when(source.poll()).thenReturn(Optional.empty());
        when(source.isEmpty()).thenReturn(true);
        when(source.closed()).thenReturn(true);
        when(source.error()).thenReturn(Optional.of(ErrorMessage.getDefaultInstance()));

        writer.request(3);

        verify(destination).completeWithError(ErrorMessage.getDefaultInstance());
    }

    @Test
    void testRequestWhenSourceCompleted() {
        when(source.poll()).thenReturn(Optional.empty());
        when(source.isEmpty()).thenReturn(true);
        when(source.closed()).thenReturn(true);
        when(source.error()).thenReturn(Optional.empty());

        writer.request(3);

        verify(destination).complete();
    }

    @Test
    void completionIsSentWhenBufferClosesWhileAnotherThreadHoldsTheFlowGate() {
        BlockingCloseableBuffer<String> real = new BlockingCloseableBuffer<>();
        DisposableReadonlyBuffer<String> hooked = closingDuringClosedCheck(real, real::close);
        writer = new FlowControlledReplyChannelWriter<>(hooked, destination);
        real.put("response");

        writer.request(32);

        verify(destination, times(1)).send("response");
        verify(destination, times(1)).complete();
        verify(destination, never()).completeWithError(any(ErrorMessage.class));
    }

    @Test
    void errorCompletionIsSentWhenBufferClosesExceptionallyWhileAnotherThreadHoldsTheFlowGate() {
        BlockingCloseableBuffer<String> real = new BlockingCloseableBuffer<>();
        ErrorMessage error = ErrorMessage.newBuilder().setMessage("failure").build();
        DisposableReadonlyBuffer<String> hooked =
                closingDuringClosedCheck(real, () -> real.closeExceptionally(error));
        writer = new FlowControlledReplyChannelWriter<>(hooked, destination);
        real.put("response");

        writer.request(32);

        verify(destination, times(1)).send("response");
        verify(destination, times(1)).completeWithError(error);
        verify(destination, never()).complete();
    }

    @Test
    void testCancelPropagation() {
        writer.cancel();
        verify(source).dispose();
    }

    /**
     * Wraps the given {@code real} buffer so that the first {@code closed()} check after the buffer has been drained
     * triggers the given {@code closer} on a second thread, whilst still returning the value from before the close.
     * This forces the interleaving in which the closing thread loses the flow gate CAS in the writer.
     */
    private static DisposableReadonlyBuffer<String> closingDuringClosedCheck(BlockingCloseableBuffer<String> real,
                                                                             Runnable closer) {
        AtomicBoolean drained = new AtomicBoolean();
        AtomicBoolean injected = new AtomicBoolean();
        return new DisposableReadonlyBuffer<String>() {
            @Override
            public Optional<String> poll() {
                Optional<String> result = real.poll();
                if (result.isPresent()) {
                    drained.set(true);
                }
                return result;
            }

            @Override
            public boolean isEmpty() {
                return real.isEmpty();
            }

            @Override
            public int capacity() {
                return real.capacity();
            }

            @Override
            public void onAvailable(Runnable onAvailable) {
                real.onAvailable(onAvailable);
            }

            @Override
            public void dispose() {
                // noop
            }

            @Override
            public Optional<ErrorMessage> error() {
                return real.error();
            }

            @Override
            public boolean closed() {
                boolean closed = real.closed();
                if (drained.get() && !closed && injected.compareAndSet(false, true)) {
                    Thread producer = new Thread(closer, "producer");
                    producer.start();
                    try {
                        producer.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
                return closed;
            }
        };
    }
}
