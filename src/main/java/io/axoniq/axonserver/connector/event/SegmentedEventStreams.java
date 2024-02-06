package io.axoniq.axonserver.connector.event;

import java.util.function.IntConsumer;

public interface SegmentedEventStreams  {

    void onSegmentClosed(IntConsumer callback);
    void onSegmentOpened(IntConsumer callback);

    SegmentEventStream segment(int segment);

    void close();
}
