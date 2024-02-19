package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.EventWithToken;

/**
 * An event stream producing events for one segment of a persistent stream.
 */
public interface PersistentStreamSegment extends ResultStream<EventWithToken> {

    /**
     * Registers a callback that will be invoked when Axon Server closes the segment within the persistent
     * stream connection. This happens when the number of segments in the persistent stream has changed or when
     * Axon Server assigns a segment to another client.
     * @param callback the callback to register
     */
    void onSegmentClosed(Runnable callback);

    /**
     * Sends the last processed token for this segment to Axon Server. Clients may choose to notify each processed event,
     * or to only sent progress information after a number of processed events.
     * @param token the last processed token for this segment
     */
    void acknowledge(long token);

    /**
     * Returns the segment number of the stream.
     * @return the segment number of this stream
     */
    int segment();
}
