package io.axoniq.axonserver.connector.impl;

public class StreamClosedException extends RuntimeException {
    public StreamClosedException(Throwable cause) {
        super(cause);
    }
}
