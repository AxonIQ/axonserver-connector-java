package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;

public class BufferedEventStream extends AbstractBufferedStream<EventWithToken, GetEventsRequest> {

    public static final EventWithToken TERMINAL_MESSAGE = EventWithToken.newBuilder().setToken(-1729).build();

    public BufferedEventStream(int bufferSize, int refillBatch) {
        super("unused", bufferSize, refillBatch);
    }

    @Override
    protected GetEventsRequest buildFlowControlMessage(FlowControl flowControl) {
        return GetEventsRequest.newBuilder().setNumberOfPermits(flowControl.getPermits()).build();
    }

    @Override
    protected EventWithToken terminalMessage() {
        return TERMINAL_MESSAGE;
    }

}
