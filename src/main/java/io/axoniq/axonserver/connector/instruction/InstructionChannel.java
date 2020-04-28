package io.axoniq.axonserver.connector.instruction;

import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;

public interface InstructionChannel {

    Runnable registerInstructionHandler(PlatformOutboundInstruction.RequestCase type, InstructionHandler handler);
}
