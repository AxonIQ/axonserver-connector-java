package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Event;

public interface SegmentEventWithToken {
    int segment();

    Event event();

    long token();

}
