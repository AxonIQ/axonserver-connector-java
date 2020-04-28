package io.axoniq.axonserver.connector;

import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.instruction.InstructionChannel;

public interface AxonServerConnection {

    boolean isConnected();

    void disconnect();

    InstructionChannel instructionChannel();

    CommandChannel commandChannel();

    EventChannel eventChannel();
}
