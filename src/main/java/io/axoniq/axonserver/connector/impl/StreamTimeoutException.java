package io.axoniq.axonserver.connector.impl;

/**
 * A {@link RuntimeException} to throw if reading from a stream results in a timeout.
 *
 * @author Mitchell Herrijgers
 * @since 4.5.6
 */
public class StreamTimeoutException extends RuntimeException {

    /**
     * Constructs a new instance of {@link StreamTimeoutException}.
     */
    public StreamTimeoutException() {
    }

    /**
     * Constructs a new instance of {@link StreamTimeoutException} with a detailed description of the cause of the
     * exception
     *
     * @param message the message describing the cause of the exception.
     */
    public StreamTimeoutException(String message) {
        super(message);
    }
}
