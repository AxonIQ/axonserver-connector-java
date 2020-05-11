package io.axoniq.axonserver.connector;

import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.instruction.InstructionChannel;
import io.axoniq.axonserver.connector.query.QueryChannel;

public interface AxonServerConnection {

    boolean isReady();

    boolean isConnected();

    void disconnect();

    InstructionChannel instructionChannel();

    CommandChannel commandChannel();

    EventChannel eventChannel();

    QueryChannel queryChannel();
}
