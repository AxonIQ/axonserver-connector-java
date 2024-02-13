package io.axoniq.axonserver.connector.event;

import java.util.function.Consumer;

public interface PersistentStream {

    void onSegmentOpened(Consumer<EventStreamSegment> callback);

    void close();

    void onClosed(Consumer<Throwable> closedCallback);
}
