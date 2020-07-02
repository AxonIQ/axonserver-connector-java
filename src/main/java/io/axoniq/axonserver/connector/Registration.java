package io.axoniq.axonserver.connector;

/**
 * Interface describing a registration that can be cancelled.
 */
@FunctionalInterface
public interface Registration {

    /**
     * Cancel the registration from which this instance was returned. Does nothing if the registration has already
     * been cancelled, or when the registration was undone by another mechanism (such as a new registration overriding
     * this one).
     */
    void cancel();

}
