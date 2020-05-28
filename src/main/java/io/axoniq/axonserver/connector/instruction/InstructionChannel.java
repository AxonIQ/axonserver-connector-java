package io.axoniq.axonserver.connector.instruction;

import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public interface InstructionChannel {

    Registration registerInstructionHandler(PlatformOutboundInstruction.RequestCase type, InstructionHandler handler);

    Registration registerEventProcessor(String processorName,
                                        Supplier<EventProcessorInfo> infoSupplier,
                                        ProcessorInstructionHandler instructionHandler);

    void enableHeartbeat(long interval, long timeout, TimeUnit timeUnit);

    void disableHeartbeat();

}
