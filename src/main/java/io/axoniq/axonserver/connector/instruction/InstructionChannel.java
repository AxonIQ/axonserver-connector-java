package io.axoniq.axonserver.connector.instruction;

import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;

import java.util.concurrent.TimeUnit;

public interface InstructionChannel {

    Registration registerInstructionHandler(PlatformOutboundInstruction.RequestCase type, InstructionHandler handler);

    void enableHeartbeat(long interval, long timeout, TimeUnit timeUnit);

    void disableHeartbeat();

}
