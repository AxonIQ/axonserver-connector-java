/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl.buffer;

import io.axoniq.axonserver.connector.FlowControl;
import io.axoniq.axonserver.connector.impl.CloseableReadonlyBuffer;
import io.axoniq.axonserver.grpc.ErrorMessage;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the {@link FlowControlledDisposableReadonlyBuffer}.
 */
class FlowControlledDisposableReadonlyBufferTest {

    private FlowControl flowControl;
    private CloseableReadonlyBuffer<String> buffer;
    private FlowControlledDisposableReadonlyBuffer<String> flowControlledBuffer;

    @BeforeEach
    void setUp() {
        flowControl = mock(FlowControl.class);
        buffer = mock(CloseableReadonlyBuffer.class);
        flowControlledBuffer = new FlowControlledDisposableReadonlyBuffer<>(flowControl, buffer);
    }

    @Test
    void testPolling() {
        when(buffer.poll()).thenReturn(Optional.of("v1"))
                           .thenReturn(Optional.empty())
                           .thenReturn(Optional.of("v2"))
                           .thenReturn(Optional.of("v3"));
        when(buffer.capacity()).thenReturn(5);
        when(buffer.closed()).thenReturn(false)
                             .thenReturn(false)
                             .thenReturn(true);

        assertEquals(Optional.of("v1"), flowControlledBuffer.poll());
        assertEquals(Optional.empty(), flowControlledBuffer.poll());
        assertEquals(Optional.of("v2"), flowControlledBuffer.poll());
        assertEquals(Optional.of("v3"), flowControlledBuffer.poll());

        verify(flowControl).request(5); // initial
        verify(flowControl, times(2)).request(1); // replenish
        verifyNoMoreInteractions(flowControl); // no more replenishment, buffer is closed
    }

    @Test
    void testIsEmpty() {
        when(buffer.isEmpty()).thenReturn(false)
                              .thenReturn(true);

        assertFalse(flowControlledBuffer.isEmpty());
        assertTrue(flowControlledBuffer.isEmpty());
    }

    @Test
    void testCapacity() {
        when(buffer.capacity()).thenReturn(23);

        assertEquals(23, flowControlledBuffer.capacity());
    }

    @Test
    void testOnAvailablePropagated() {
        Runnable onAvailable = () -> {
        };
        flowControlledBuffer.onAvailable(onAvailable);

        verify(buffer).onAvailable(onAvailable);
    }

    @Test
    void testDisposePropagated() {
        flowControlledBuffer.dispose();

        verify(flowControl).cancel();
    }

    @Test
    void testClosed() {
        when(buffer.closed()).thenReturn(false)
                             .thenReturn(true);

        assertFalse(flowControlledBuffer.closed());
        assertTrue(flowControlledBuffer.closed());
    }

    @Test
    void testError() {
        when(buffer.error()).thenReturn(Optional.of(ErrorMessage.getDefaultInstance()));

        assertEquals(Optional.of(ErrorMessage.getDefaultInstance()), flowControlledBuffer.error());
    }
}
