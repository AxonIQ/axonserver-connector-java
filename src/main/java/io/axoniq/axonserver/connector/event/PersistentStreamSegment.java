/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.streams.PersistentStreamEvent;

import java.util.Optional;

/**
 * An event stream producing events for one segment of a persistent stream.
 *
 * @author Marc Gathier
 * @since 2024.0.0
 */
public interface PersistentStreamSegment extends ResultStream<PersistentStreamEvent> {

    /**
     * Acknowledgement value to indicate that all events processed after a segment is closed
     */
    long PENDING_WORK_DONE_MARKER = -45;

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


    void error(String error);

    /**
     * Returns the segment number of the stream.
     * @return the segment number of this stream
     */
    int segment();

}
