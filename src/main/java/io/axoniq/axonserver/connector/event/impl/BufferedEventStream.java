package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.PayloadDescription;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.nonNullOrDefault;

public class BufferedEventStream extends AbstractBufferedStream<EventWithToken, GetEventsRequest> implements EventStream {

    public static final EventWithToken TERMINAL_MESSAGE = EventWithToken.newBuilder().setToken(-1729).build();
    private final long trackingToken;
    private final boolean forceReadFromLeader;

    public BufferedEventStream(long trackingToken, int bufferSize, int refillBatch, boolean forceReadFromLeader) {
        super("unused", bufferSize, refillBatch);
        this.trackingToken = trackingToken;
        this.forceReadFromLeader = forceReadFromLeader;
    }

    @Override
    protected GetEventsRequest buildFlowControlMessage(FlowControl flowControl) {
        return GetEventsRequest.newBuilder().setNumberOfPermits(flowControl.getPermits()).build();
    }

    @Override
    protected GetEventsRequest buildInitialFlowControlMessage(FlowControl flowControl) {
        return GetEventsRequest.newBuilder()
                               .setTrackingToken(trackingToken)
                               .setAllowReadingFromFollower(!forceReadFromLeader)
                               .setNumberOfPermits(flowControl.getPermits()).build();
    }

    @Override
    protected EventWithToken terminalMessage() {
        return TERMINAL_MESSAGE;
    }

    @Override
    public void excludePayloadType(String payloadType, String revision) {
        outboundStream().onNext(
                GetEventsRequest.newBuilder()
                                .addBlacklist(PayloadDescription.newBuilder()
                                                                .setType(payloadType)
                                                                .setRevision(nonNullOrDefault(revision, ""))
                                                                .build())
                                .build());
    }

}
