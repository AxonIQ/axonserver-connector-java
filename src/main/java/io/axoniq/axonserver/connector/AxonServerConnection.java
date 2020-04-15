package io.axoniq.axonserver.connector;

public interface AxonServerConnection {

    boolean isConnected();

    void disconnect();

    InstructionChannel instructionChannel();

    CommandChannel commandChannel();
}
