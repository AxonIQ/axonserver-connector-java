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

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.InstructionHandler;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.control.ProcessorInstructionHandler;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.axoniq.axonserver.connector.impl.MessageFactory.buildErrorMessage;

/**
 * Utility class providing {@link InstructionHandler} instances for event processor instructions.
 */
public abstract class ProcessorInstructions {

    private ProcessorInstructions() {
        // Utility class
    }

    /**
     * Provides a {@link InstructionHandler} which handles a processor's merge request from AxonServer and sends it to
     * the right client(s).
     *
     * @param instructionHandlers the collection of instruction handler instances to retrieve the {@link
     *                            ProcessorInstructionHandler} from to execute the actual {@link
     *                            ProcessorInstructionHandler#mergeSegment(int)} operation
     * @return the {@link InstructionHandler} to delegate the merge request with
     */
    public static InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> mergeHandler(
            Map<String, ProcessorInstructionHandler> instructionHandlers
    ) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getMergeEventProcessorSegment().getProcessorName();
            int segmentId = instruction.getMergeEventProcessorSegment().getSegmentIdentifier();
            executeAndReply(replyChannel, instructionHandlers.get(processorName), h -> h.mergeSegment(segmentId));
        };
    }

    /**
     * Provides a {@link InstructionHandler} which handles a processor's split request from AxonServer and sends it to
     * the right client(s).
     *
     * @param instructionHandlers the collection of instruction handler instances to retrieve the {@link
     *                            ProcessorInstructionHandler} from to execute the actual {@link
     *                            ProcessorInstructionHandler#splitSegment(int)} operation
     * @return the {@link InstructionHandler} to delegate the split request with
     */
    public static InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> splitHandler(
            Map<String, ProcessorInstructionHandler> instructionHandlers
    ) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getSplitEventProcessorSegment().getProcessorName();
            int segmentId = instruction.getSplitEventProcessorSegment().getSegmentIdentifier();
            executeAndReply(replyChannel, instructionHandlers.get(processorName), h -> h.splitSegment(segmentId));
        };
    }

    /**
     * Provides a {@link InstructionHandler} which handles a processor's start request from AxonServer and sends it to
     * the right client(s).
     *
     * @param instructionHandlers the collection of instruction handler instances to retrieve the {@link
     *                            ProcessorInstructionHandler} from to execute the actual {@link
     *                            ProcessorInstructionHandler#startProcessor()} operation
     * @return the {@link InstructionHandler} to delegate the start request with
     */
    public static InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> startHandler(
            Map<String, ProcessorInstructionHandler> instructionHandlers
    ) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getStartEventProcessor().getProcessorName();
            executeAndReply(
                    replyChannel, instructionHandlers.get(processorName), h -> h.startProcessor().thenApply(r -> true)
            );
        };
    }

    /**
     * Provides a {@link InstructionHandler} which handles a processor's pause request from AxonServer and sends it to
     * the right client(s).
     *
     * @param instructionHandlers the collection of instruction handler instances to retrieve the {@link
     *                            ProcessorInstructionHandler} from to execute the actual {@link
     *                            ProcessorInstructionHandler#pauseProcessor()} operation
     * @return the {@link InstructionHandler} to delegate the pause request with
     */
    public static InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> pauseHandler(
            Map<String, ProcessorInstructionHandler> instructionHandlers
    ) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getPauseEventProcessor().getProcessorName();
            executeAndReply(
                    replyChannel, instructionHandlers.get(processorName), h -> h.pauseProcessor().thenApply(r -> true)
            );
        };
    }

    /**
     * Provides a {@link InstructionHandler} which handles a processor's release segment request from AxonServer and
     * sends it to the right client(s).
     *
     * @param instructionHandlers the collection of instruction handler instances to retrieve the {@link
     *                            ProcessorInstructionHandler} from to execute the actual {@link
     *                            ProcessorInstructionHandler#releaseSegment(int)}} operation
     * @return the {@link InstructionHandler} to delegate the release segment request with
     */
    public static InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> releaseSegmentHandler(
            Map<String, ProcessorInstructionHandler> instructionHandlers
    ) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getReleaseSegment().getProcessorName();
            int segmentId = instruction.getReleaseSegment().getSegmentIdentifier();
            executeAndReply(replyChannel, instructionHandlers.get(processorName), h -> h.releaseSegment(segmentId));
        };
    }

    /**
     * Provides a {@link InstructionHandler} which handles a processor's information request from AxonServer and sends
     * it to the right client(s).
     *
     * @param infoSuppliers the collection of {@link EventProcessorInfo} supplier instances to retrieve the right
     *                      instance to pull information from
     * @return the {@link InstructionHandler} to delegate the processor information request with
     */
    public static InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> requestInfoHandler(
            Map<String, Supplier<EventProcessorInfo>> infoSuppliers
    ) {
        return (instruction, replyChannel) -> {
            String instructionId = instruction.getInstructionId();
            String processorName = instruction.getRequestEventProcessorInfo().getProcessorName();
            Supplier<EventProcessorInfo> infoSupplier = infoSuppliers.get(processorName);
            if (infoSupplier != null) {
                EventProcessorInfo processorInfo = infoSupplier.get();
                replyChannel.send(PlatformInboundInstruction.newBuilder()
                                                            .setInstructionId(instructionId)
                                                            .setEventProcessorInfo(processorInfo)
                                                            .build());
            }
            replyChannel.complete();
        };
    }

    private static void executeAndReply(ReplyChannel<PlatformInboundInstruction> replyChannel,
                                        ProcessorInstructionHandler handler,
                                        Function<ProcessorInstructionHandler, CompletableFuture<Boolean>> task) {
        if (handler != null) {
            task.apply(handler)
                .whenComplete((r, e) -> {
                    boolean success = r != null && r;
                    if (!success || e != null) {
                        replyChannel.completeWithError(
                                buildErrorMessage(ErrorCategory.INSTRUCTION_EXECUTION_ERROR, "client", e)
                        );
                    } else {
                        replyChannel.complete();
                    }
                });
        } else {
            replyChannel.completeWithError(ErrorCategory.INSTRUCTION_EXECUTION_ERROR, "Unknown processor");
        }
    }
}
