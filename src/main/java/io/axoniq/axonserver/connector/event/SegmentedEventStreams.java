package io.axoniq.axonserver.connector.event;

import java.util.function.Consumer;

public interface SegmentedEventStreams  {

    void onSegmentOpened(Consumer<SegmentEventStream> callback);

    void close();

    void onClosed(Consumer<Throwable> closedCallback);
}
