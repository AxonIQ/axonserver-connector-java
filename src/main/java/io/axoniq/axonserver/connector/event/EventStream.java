package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.EventWithToken;

/**
 * Represents a stream of Events. It can be consumed in a blocking and non-blocking approach. Each event is accompanied
 * with a token, which can be used to reopen a new stream at the same position.
 */
public interface EventStream extends ResultStream<EventWithToken> {

    /**
     * Instructs AxonServer to not send messages with the given {@code payloadType} and {@code revision}. This method
     * is in no way a guarantee that these message will no longer be sent at all. Some messages may already have been
     * en-route, and AxonServer may decide to send messages every once in a while as a "beacon" to relay progress of
     * the tracking token. It may choose, however, to omit the actual payload, in that case.
     *
     * @param payloadType The type of payload to exclude from the stream
     * @param revision    The revision of the payload to exclude from the stream
     */
    void excludePayloadType(String payloadType, String revision);

}
