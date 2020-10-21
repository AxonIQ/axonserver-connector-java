package io.axoniq.axonserver.connector.impl;

/**
 * A {@link RuntimeException} to throw if a stream is completed unexpectedly.
 *
 * @author Sara Pellegrini
 * @since 4.4.3
 */
public class StreamUnexpectedlyCompletedException extends RuntimeException {


    /**
     * Constructs a new instance of {@link StreamUnexpectedlyCompletedException}.
     */
    public StreamUnexpectedlyCompletedException() {
    }

    /**
     * Constructs a new instance of {@link StreamUnexpectedlyCompletedException} with a detailed description of the
     * cause of the exception
     *
     * @param message the message describing the cause of the exception.
     */
    public StreamUnexpectedlyCompletedException(String message) {
        super(message);
    }
}
