package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;

public class BufferOverflowException extends AxonServerException {

    public BufferOverflowException(String message) {
        super(ErrorCategory.OTHER, message);
    }
}
