package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.connector.impl.FlowControlledBuffer;
import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;

import java.util.concurrent.TimeUnit;

public class BufferedEventStream extends FlowControlledBuffer<EventWithToken, GetEventsRequest> implements EventStream {

    public static final EventWithToken TERMINAL_MESSAGE = EventWithToken.newBuilder().setToken(-1729).build();

    private volatile Runnable onAvailableCallback = () -> {};

    public BufferedEventStream(int bufferSize, int refillBatch) {
        super("unused", TERMINAL_MESSAGE, bufferSize, refillBatch);
    }

    @Override
    protected GetEventsRequest buildFlowControlMessage(FlowControl flowControl) {
        return GetEventsRequest.newBuilder().setNumberOfPermits(flowControl.getPermits()).build();
    }

    @Override
    public EventWithToken next() throws InterruptedException, StreamClosedException {
        return take();
    }

    @Override
    public EventWithToken nextIfAvailable() {
        return tryTakeNow();
    }

    @Override
    public EventWithToken nextIfAvailable(int timeout, TimeUnit unit) throws InterruptedException {
        return poll(timeout, unit);
    }

    @Override
    public void onNext(EventWithToken value) {
        super.onNext(value);
        onAvailableCallback.run();
    }

    @Override
    public EventWithToken peek() {
        return super.peek();
    }

    @Override
    public void onAvailable(Runnable callback) {
        if (callback == null) {
            onAvailableCallback = () -> {};
        } else {
            onAvailableCallback = callback;
        }
    }

    @Override
    public boolean isClosed() {
        return super.isClosed();
    }

    @Override
    public void close() {
        outboundStream().onCompleted();
    }

}
