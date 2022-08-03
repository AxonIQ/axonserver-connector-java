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

package io.axoniq.axonserver.connector.impl.buffer;

import io.axoniq.axonserver.grpc.ErrorMessage;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the {@link BlockingCloseableBuffer}.
 */
class BlockingCloseableBufferTest {

    private BlockingCloseableBuffer<String> buffer;

    @Test
    void testDefaultCapacity() {
        assertEquals(32, buffer.capacity());
    }

    @BeforeEach
    void setUp() {
        buffer = new BlockingCloseableBuffer<>();
    }

    @Test
    void testClosing() {
        assertFalse(buffer.closed());

        buffer.close();

        assertTrue(buffer.closed());
    }

    @Test
    void testClosingExceptionally() {
        assertFalse(buffer.closed());

        ErrorMessage errorMessage = ErrorMessage.getDefaultInstance();
        buffer.closeExceptionally(errorMessage);

        assertTrue(buffer.closed());
        Optional<ErrorMessage> error = buffer.error();
        assertEquals(Optional.of(errorMessage), error);
    }

    @Test
    void testOnAvailableAttachedBeforeClosing() {
        Runnable onAvailable = mock(Runnable.class);
        buffer.onAvailable(onAvailable);

        buffer.close();

        verify(onAvailable).run();
    }

    @Test
    void testOnAvailableAttachedAfterClosing() {
        Runnable onAvailable = mock(Runnable.class);

        buffer.close();
        buffer.onAvailable(onAvailable);

        verify(onAvailable).run();
    }

    @Test
    void testOnAvailableAttachedBeforeClosingExceptionally() {
        Runnable onAvailable = mock(Runnable.class);
        buffer.onAvailable(onAvailable);

        buffer.closeExceptionally(ErrorMessage.getDefaultInstance());

        verify(onAvailable).run();
    }

    @Test
    void testOnAvailableAttachedAfterClosingExceptionally() {
        Runnable onAvailable = mock(Runnable.class);

        buffer.closeExceptionally(ErrorMessage.getDefaultInstance());
        buffer.onAvailable(onAvailable);

        verify(onAvailable).run();
    }

    @Test
    void testPollingFromEmptyBuffer() {
        assertTrue(buffer.isEmpty());
        assertFalse(buffer.poll().isPresent());
    }

    @Test
    void testOnAvailableAttachedBeforePut() {
        Runnable onAvailable = mock(Runnable.class);
        buffer.onAvailable(onAvailable);

        buffer.put("element");

        verify(onAvailable).run();
    }

    @Test
    void testOnAvailableAttachedAfterPut() {
        Runnable onAvailable = mock(Runnable.class);

        buffer.put("element");
        buffer.onAvailable(onAvailable);

        verify(onAvailable).run();
    }

    @Test
    void testPutAndPoll() {
        buffer.put("element");
        assertEquals(1, buffer.size());
        assertEquals(Optional.of("element"), buffer.poll());
        assertEquals(0, buffer.size());
    }

    @Test
    void testConcurrentPutAndPoll() {
        AtomicInteger putCounter = new AtomicInteger();
        Object putLock = new Object();
        Runnable putAction = () -> {
            synchronized (putLock) {
                buffer.put("" + putCounter.getAndIncrement());
            }
        };

        Object pollLock = new Object();
        AtomicBoolean validity = new AtomicBoolean(true);
        ConcurrentLinkedQueue<String> polled = new ConcurrentLinkedQueue<>();
        Runnable pollAction = () -> {
            synchronized (pollLock) {
                validity.set(validity.get() && buffer.size() <= buffer.capacity());
                buffer.poll()
                      .ifPresent(polled::add);
            }
        };

        int repetition = 1_000;
        while (polled.size() < repetition) {
            Runnable action = ThreadLocalRandom.current().nextInt() % 3 == 0 ? pollAction : putAction;
            new Thread(action).start();
        }

        assertTrue(validity.get(), "Buffer overflow.");

        assertTrue(increasing(polled.stream()
                                    .map(Integer::valueOf)
                                    .collect(Collectors.toList())));
    }

    private boolean increasing(List<Integer> elements) {
        for (int i = 0; i < elements.size() - 1; i++) {
            if (elements.get(i) + 1 != elements.get(i + 1)) {
                return false;
            }
        }
        return true;
    }
}