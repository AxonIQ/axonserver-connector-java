/*
 * Copyright (c) 2020. AxonIQ
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

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.EventWithToken;

/**
 * Represents a stream of Events. It can be consumed in a blocking and non-blocking approach. Each event is accompanied
 * with a token, which can be used to reopen a new stream at the same position.
 */
public interface EventStream extends ResultStream<EventWithToken> {

    /**
     * Instructs AxonServer to <em>not</em> send messages with the given {@code payloadType} and {@code revision}. This
     * method is in no way a guarantee that these message will no longer be sent at all. Some messages may already have
     * been en-route, and AxonServer may decide to send messages every once in a while as a "beacon" to relay progress
     * of the tracking token. It may choose, however, to omit the actual payload, in that case.
     *
     * @param payloadType the type of payload to exclude from the stream
     * @param revision    the revision of the payload to exclude from the stream
     */
    void excludePayloadType(String payloadType, String revision);
}
