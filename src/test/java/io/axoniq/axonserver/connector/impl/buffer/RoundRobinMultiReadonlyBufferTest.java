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

import io.axoniq.axonserver.connector.impl.DisposableReadonlyBuffer;
import io.axoniq.axonserver.grpc.ErrorMessage;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the {@link RoundRobinMultiReadonlyBuffer}.
 */
class RoundRobinMultiReadonlyBufferTest {

    private DisposableReadonlyBuffer<String> buffer1;
    private DisposableReadonlyBuffer<String> buffer2;
    private DisposableReadonlyBuffer<String> buffer3;

    private RoundRobinMultiReadonlyBuffer<String> roundRobin;

    @BeforeEach
    void setUp() {
        buffer1 = mock(DisposableReadonlyBuffer.class);
        buffer2 = mock(DisposableReadonlyBuffer.class);
        buffer3 = mock(DisposableReadonlyBuffer.class);
        roundRobin = new RoundRobinMultiReadonlyBuffer<>(asList(buffer1, buffer2, buffer3));
    }

    @Test
    void testCreationWithNullBuffers() {
        assertThrows(IllegalArgumentException.class, () -> new RoundRobinMultiReadonlyBuffer<>(null));
    }

    @Test
    void testCreationWithNoBuffers() {
        assertThrows(IllegalArgumentException.class, () -> new RoundRobinMultiReadonlyBuffer<>(emptyList()));
    }

    @Test
    void testRoundRobinPolling() {
        when(buffer1.poll()).thenReturn(Optional.of("b1"));
        when(buffer2.poll()).thenReturn(Optional.of("b2"));
        when(buffer3.poll()).thenReturn(Optional.of("b3"));

        for (int i = 0; i < 100; i++) {
            assertEquals(Optional.of("b1"), roundRobin.poll());
            assertEquals(Optional.of("b2"), roundRobin.poll());
            assertEquals(Optional.of("b3"), roundRobin.poll());
        }
    }

    @Test
    void testRoundRobinPollingWhenAllBuffersAreEmpty() {
        when(buffer1.poll()).thenReturn(Optional.empty());
        when(buffer2.poll()).thenReturn(Optional.empty());
        when(buffer3.poll()).thenReturn(Optional.empty());

        for (int i = 0; i < 100; i++) {
            assertEquals(Optional.empty(), roundRobin.poll());
        }
    }

    @Test
    void testRoundRobinPollingWhenSomeBuffersAreEmpty() {
        when(buffer1.poll()).thenReturn(Optional.of("b11"))
                            .thenReturn(Optional.of("b12"))
                            .thenReturn(Optional.empty())
                            .thenReturn(Optional.of("b13"));

        when(buffer2.poll()).thenReturn(Optional.empty());

        when(buffer3.poll()).thenReturn(Optional.of("b21"))
                            .thenReturn(Optional.empty())
                            .thenReturn(Optional.empty())
                            .thenReturn(Optional.of("b23"));

        assertEquals(Optional.of("b11"), roundRobin.poll());
        assertEquals(Optional.of("b21"), roundRobin.poll());
        assertEquals(Optional.of("b12"), roundRobin.poll());
        assertEquals(Optional.empty(), roundRobin.poll());
        assertEquals(Optional.of("b13"), roundRobin.poll());
        assertEquals(Optional.of("b23"), roundRobin.poll());
    }

    @Test
    void testCloseWhenNotAllBuffersAreClosed() {
        when(buffer1.closed()).thenReturn(false);
        when(buffer2.closed()).thenReturn(true);
        when(buffer3.closed()).thenReturn(false);

        assertFalse(roundRobin.closed());
    }

    @Test
    void testCloseWhenAllBuffersAreClosed() {
        when(buffer1.closed()).thenReturn(true);
        when(buffer2.closed()).thenReturn(true);
        when(buffer3.closed()).thenReturn(true);

        assertTrue(roundRobin.closed());
    }

    @Test
    void testErrorWhenNotAllBuffersReturnAnError() {
        when(buffer1.error()).thenReturn(Optional.of(ErrorMessage.getDefaultInstance()));
        when(buffer2.error()).thenReturn(Optional.of(ErrorMessage.getDefaultInstance()));
        when(buffer3.error()).thenReturn(Optional.empty());

        assertEquals(Optional.empty(), roundRobin.error());
    }

    @Test
    void testErrorWhenAllBuffersReturnAnError() {
        ErrorMessage buffer1Error = ErrorMessage.newBuilder()
                                                .setMessage("msg")
                                                .setErrorCode("errorCode")
                                                .setLocation("location")
                                                .addDetails("detail")
                                                .build();
        when(buffer1.error()).thenReturn(Optional.of(buffer1Error));
        when(buffer2.error()).thenReturn(Optional.of(ErrorMessage.getDefaultInstance()));
        when(buffer3.error()).thenReturn(Optional.of(ErrorMessage.getDefaultInstance()));

        assertEquals(Optional.of(buffer1Error), roundRobin.error());
    }

    @Test
    void testEmptinessWhenNotAllBuffersAreEmpty() {
        when(buffer1.isEmpty()).thenReturn(false);
        when(buffer2.isEmpty()).thenReturn(true);
        when(buffer3.isEmpty()).thenReturn(false);

        assertFalse(roundRobin.isEmpty());
    }

    @Test
    void testEmptinessWhenAllBuffersAreEmpty() {
        when(buffer1.isEmpty()).thenReturn(true);
        when(buffer2.isEmpty()).thenReturn(true);
        when(buffer3.isEmpty()).thenReturn(true);

        assertTrue(roundRobin.isEmpty());
    }

    @Test
    void testCapacity() {
        when(buffer1.capacity()).thenReturn(4);
        when(buffer2.capacity()).thenReturn(7);
        when(buffer3.capacity()).thenReturn(2);

        assertEquals(13, roundRobin.capacity());
    }

    @Test
    void testOnAvailablePropagation() {
        Runnable onAvailable = () -> {
        };
        roundRobin.onAvailable(onAvailable);

        verify(buffer1).onAvailable(onAvailable);
        verify(buffer2).onAvailable(onAvailable);
        verify(buffer3).onAvailable(onAvailable);
    }

    @Test
    void testDisposePropagation() {
        roundRobin.dispose();

        verify(buffer1).dispose();
        verify(buffer2).dispose();
        verify(buffer3).dispose();
    }
}
