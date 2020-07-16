/*
 * Copyright (c) 2020. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.control;

import io.axoniq.axonserver.connector.InstructionHandler;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Communication channel for interactions with AxonServer related to control messages and statistics.
 */
public interface ControlChannel {

    /**
     * Registers the given {@code handler} to handle a given {@code type} of instruction. Any previous registrations of
     * handlers for the given type of instruction are overwritten.
     * <p>
     * Note that de-registration of a handler will not reinstate any previously registered handler for the same type of
     * instruction.
     * <p>
     * This method is intended to overwrite the default behavior for incoming instructions, or to provide support for
     * non-default instructions.
     *
     * @param type    the type of instructions to handle
     * @param handler the handler to invoke for incoming instructions
     * @return a handle to unregister this instruction handler
     */
    Registration registerInstructionHandler(PlatformOutboundInstruction.RequestCase type,
                                            InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> handler);

    /**
     * Registers an Event Processor with AxonServer, allowing AxonServer to request status information and provide
     * instructions for the Processor.
     *
     * @param processorName      the name of the processor
     * @param infoSupplier       supplier for up-to-date status information of the processor
     * @param instructionHandler handler for incoming instructions for the processor
     * @return a handle to cancel the registration of the event processor
     */
    Registration registerEventProcessor(String processorName,
                                        Supplier<EventProcessorInfo> infoSupplier,
                                        ProcessorInstructionHandler instructionHandler);

    /**
     * Enables sending heartbeat message to validate that the connection to AxonServer is alive. This ensures a fully
     * operational end-to-end connection with AxonServer.
     * <p>
     * When enabled, the client will abandon any connections on which no timely response to a heartbeat has been
     * received. If heartbeats are also enabled on the AxonServer side, any heartbeat initiated by AxonServer will count
     * as a valid connection confirmation.
     * <p>
     * Consecutive invocations of this method will alter the configuration, resetting heartbeat timers and reinitialize
     * the heartbeat processing.
     *
     * @param interval the interval at which heartbeat messages are expected
     * @param timeout  the maximum time to wait for a confirmation after initiating a heartbeat message
     * @param timeUnit the unit of time in which interval and timeout are expressed
     */
    void enableHeartbeat(long interval, long timeout, TimeUnit timeUnit);

    /**
     * Disable any previously enabled heartbeats. Heartbeat requests initiated by AxonServer will still be reacted to,
     * but the client will no longer initiate a heartbeat, nor close a connection when it fails to receive
     * confirmations.
     */
    void disableHeartbeat();

    /**
     * Send a the given {@code instruction} over this channel, returning the acknowledgment of the {@code instruction}
     * asynchronously.
     *
     * @param instruction the {@link PlatformInboundInstruction} to send over this channel
     * @return a {@link CompletableFuture} of the acknowledgement of the given {@code instruction}
     */
    CompletableFuture<InstructionAck> sendInstruction(PlatformInboundInstruction instruction);
}
