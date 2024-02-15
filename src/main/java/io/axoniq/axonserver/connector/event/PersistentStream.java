package io.axoniq.axonserver.connector.event;

import java.util.function.Consumer;

/**
 * A connection to a persistent stream. Axon Server can assign zero or more segments to this connection.
 */
public interface PersistentStream {

    /**
     * Registers a callback to invoke when Axon Server assigns a segment to this connection.
     * @param callback the callback to invoke when a segment is opened
     */
    void onSegmentOpened(Consumer<EventStreamSegment> callback);

    /**
     * Closes the persistent stream.
     */
    void close();

    /**
     * Registers a callback to invoke when Axon Server closes this connection (or the connection to Axon Server is lost).
     * @param closedCallback the callback to invoke when the persistent stream is closed
     */
    void onClosed(Consumer<Throwable> closedCallback);
}
