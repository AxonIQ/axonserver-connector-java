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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static org.mockito.Mockito.*;

/**
 * Tests for {@link FlowControlledReplyChannelWriter}.
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
            Runnable onAvailable = invocation.getArgumentAt(0, Runnable.class);
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
    void testCancelPropagation() {
        writer.cancel();
        verify(source).dispose();
    }
}
