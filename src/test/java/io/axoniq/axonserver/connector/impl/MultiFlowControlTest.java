package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.query.QueryHandler;
import io.axoniq.axonserver.connector.query.impl.MultiFlowControl;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link MultiFlowControl}.
 */
class MultiFlowControlTest {

    @Test
    void testRequestOnEmptyMultiFlowControl() {
        new MultiFlowControl(Collections.emptyList()).request(1);
    }

    @Test
    void testCancelOnEmptyMultiFlowControl() {
        new MultiFlowControl(Collections.emptyList()).cancel();
    }

    @Test
    void testRequestOnNonEmptyMultiFlowControl() {
        QueryHandler.FlowControl flowControl1 = mock(QueryHandler.FlowControl.class);
        QueryHandler.FlowControl flowControl2 = mock(QueryHandler.FlowControl.class);
        QueryHandler.FlowControl flowControl3 = mock(QueryHandler.FlowControl.class);
        MultiFlowControl multiFlowControl = new MultiFlowControl(asList(flowControl1, flowControl2, flowControl3));

        multiFlowControl.request(4);
        verify(flowControl1).request(4);
        verifyZeroInteractions(flowControl2);
        verifyZeroInteractions(flowControl3);

        multiFlowControl.request(5);
        verifyZeroInteractions(flowControl1);
        verify(flowControl2).request(5);
        verifyZeroInteractions(flowControl3);

        multiFlowControl.request(2);
        verifyZeroInteractions(flowControl1);
        verifyZeroInteractions(flowControl2);
        verify(flowControl3).request(2);

        multiFlowControl.request(7);
        verify(flowControl1).request(7);
        verifyZeroInteractions(flowControl2);
        verifyZeroInteractions(flowControl3);
    }

    @Test
    void testCancelOnNonEmptyMultiFlowControl() {
        QueryHandler.FlowControl flowControl1 = mock(QueryHandler.FlowControl.class);
        QueryHandler.FlowControl flowControl2 = mock(QueryHandler.FlowControl.class);
        QueryHandler.FlowControl flowControl3 = mock(QueryHandler.FlowControl.class);
        MultiFlowControl multiFlowControl = new MultiFlowControl(asList(flowControl1, flowControl2, flowControl3));

        multiFlowControl.cancel();
        verify(flowControl1).cancel();
        verify(flowControl2).cancel();
        verify(flowControl3).cancel();
    }

    @Test
    void testRequestOnNonEmptyMultiFlowControlWhenProvidedFlowControlsChanged() {
        QueryHandler.FlowControl flowControl1 = mock(QueryHandler.FlowControl.class);
        QueryHandler.FlowControl flowControl2 = mock(QueryHandler.FlowControl.class);
        QueryHandler.FlowControl flowControl3 = mock(QueryHandler.FlowControl.class);
        List<QueryHandler.FlowControl> flowControlList = new ArrayList<>(3);
        flowControlList.add(flowControl1);
        flowControlList.add(flowControl2);
        flowControlList.add(flowControl3);
        MultiFlowControl multiFlowControl = new MultiFlowControl(flowControlList);

        multiFlowControl.request(4);
        verify(flowControl1).request(4);
        verifyZeroInteractions(flowControl2);
        verifyZeroInteractions(flowControl3);

        multiFlowControl.request(5);
        verifyZeroInteractions(flowControl1);
        verify(flowControl2).request(5);
        verifyZeroInteractions(flowControl3);

        flowControlList.clear();

        multiFlowControl.request(2);
        verifyZeroInteractions(flowControl1);
        verifyZeroInteractions(flowControl2);
        verify(flowControl3).request(2);

        multiFlowControl.request(7);
        verify(flowControl1).request(7);
        verifyZeroInteractions(flowControl2);
        verifyZeroInteractions(flowControl3);
    }

    @Test
    void testWith() {
        QueryHandler.FlowControl flowControl1 = mock(QueryHandler.FlowControl.class);
        QueryHandler.FlowControl flowControl2 = mock(QueryHandler.FlowControl.class);
        QueryHandler.FlowControl flowControl3 = mock(QueryHandler.FlowControl.class);
        MultiFlowControl multiFlowControl = new MultiFlowControl(flowControl1).with(flowControl2)
                                                                              .with(flowControl3);

        assertEquals(asList(flowControl1, flowControl2, flowControl3), multiFlowControl.delegates());
    }
}
