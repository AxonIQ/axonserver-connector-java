package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.EventWithToken;

public interface SegmentEventStream extends ResultStream<EventWithToken> {
    void onSegmentClosed(Runnable callback);
    void progress( long token);

    int segment();
}
