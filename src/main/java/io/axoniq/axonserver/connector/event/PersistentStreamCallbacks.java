/*
 * Copyright (c) 2020-2024. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.axoniq.axonserver.connector.event;

import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * Definitions of the callbacks to be added to a persistent stream.
 *
 * @author Marc Gathier
 * @since 2024.0.0
 */
public class PersistentStreamCallbacks {

    private final Consumer<PersistentStreamSegment> onSegmentOpened;

    private final Consumer<PersistentStreamSegment> onSegmentClosed;
    private final Consumer<PersistentStreamSegment> onAvailable;

    private final Consumer<Throwable> onClosed;

    /**
     * Instantiates the callbacks object.
     *
     * @param onSegmentOpened callback that the connector invokes when a persistent stream segment is opened
     * @param onSegmentClosed callback that the connector invokes when a persistent stream segment is closed
     * @param onAvailable     callback that the connector invokes when an event is available on a persistent stream
     *                        segment
     * @param onClosed        callback that the connector invokes when the persistent stream is closed
     */
    public PersistentStreamCallbacks(Consumer<PersistentStreamSegment> onSegmentOpened, Consumer<PersistentStreamSegment> onSegmentClosed,
                                     Consumer<PersistentStreamSegment> onAvailable,
                                     Consumer<Throwable> onClosed) {
        this.onSegmentOpened = onSegmentOpened;
        this.onSegmentClosed = onSegmentClosed;
        this.onAvailable = onAvailable;
        this.onClosed = onClosed;
    }

    /**
     * Returns the callback that the connector invokes when the persistent stream is closed.
     *
     * @return callback that the connector invokes when the persistent stream is closed
     */
    public Consumer<Throwable> onClosed() {
        return onClosed;
    }

    /**
     * Returns the callback that the connector invokes when a persistent stream segment is opened.
     *
     * @return callback that the connector invokes when a persistent stream segment is opened
     */
    public Consumer<PersistentStreamSegment> onSegmentOpened() {
        return onSegmentOpened;
    }

    /**
     * Returns the callback that the connector invokes when a persistent stream segment is closed.
     *
     * @return callback that the connector invokes when a persistent stream segment is closed
     */
    public Consumer<PersistentStreamSegment> onSegmentClosed() {
        return onSegmentClosed;
    }

    /**
     * Returns the callback that the connector invokes when an event is available on a persistent stream segment.
     *
     * @return callback that the connector invokes when an event is available on a persistent stream segment
     */
    public Consumer<PersistentStreamSegment> onAvailable() {
        return onAvailable;
    }
}
