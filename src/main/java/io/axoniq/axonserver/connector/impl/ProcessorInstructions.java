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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.axoniq.axonserver.connector.impl.MessageFactory.buildErrorMessage;

public class ProcessorInstructions {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorInstructions.class);

    private ProcessorInstructions() {
    }

    public static InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> mergeHandler(Map<String, ProcessorInstructionHandler> instructionHandlers) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getMergeEventProcessorSegment().getProcessorName();
            int segmentId = instruction.getMergeEventProcessorSegment().getSegmentIdentifier();
            executeAndReply(replyChannel,
                            instructionHandlers.get(processorName),
                            h -> h.mergeSegment(segmentId));
        };
    }

    public static InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> splitHandler(Map<String, ProcessorInstructionHandler> instructionHandlers) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getSplitEventProcessorSegment().getProcessorName();
            int segmentId = instruction.getSplitEventProcessorSegment().getSegmentIdentifier();
            executeAndReply(replyChannel,
                            instructionHandlers.get(processorName),
                            h -> h.splitSegment(segmentId));
        };

    }

    public static InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> startHandler(Map<String, ProcessorInstructionHandler> instructionHandlers) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getStartEventProcessor().getProcessorName();
            executeAndReply(replyChannel,
                            instructionHandlers.get(processorName),
                            h -> h.startProcessor().thenApply(r -> true));
        };
    }

    public static InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> pauseHandler(Map<String, ProcessorInstructionHandler> instructionHandlers) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getPauseEventProcessor().getProcessorName();
            executeAndReply(replyChannel,
                            instructionHandlers.get(processorName),
                            h -> h.pauseProcessor().thenApply(r -> true));
        };
    }

    public static InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> releaseSegmentHandler(Map<String, ProcessorInstructionHandler> instructionHandlers) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getReleaseSegment().getProcessorName();
            int segmentId = instruction.getReleaseSegment().getSegmentIdentifier();

            executeAndReply(replyChannel,
                            instructionHandlers.get(processorName),
                            h -> h.releaseSegment(segmentId));
        };
    }

    public static InstructionHandler<PlatformOutboundInstruction, PlatformInboundInstruction> requestInfoHandler(Map<String, Supplier<EventProcessorInfo>> infoSuppliers) {
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

    private static void executeAndReply(ReplyChannel<PlatformInboundInstruction> replyChannel, ProcessorInstructionHandler handler, Function<ProcessorInstructionHandler, CompletableFuture<Boolean>> task) {
        if (handler != null) {
            task.apply(handler)
                .whenComplete((r, e) -> {
                    boolean success = r != null && r;
                    if (!success || e != null) {
                        replyChannel.completeWithError(buildErrorMessage(ErrorCategory.INSTRUCTION_EXECUTION_ERROR, "client", e));
                    } else {
                        replyChannel.complete();
                    }

                });
        } else {
            replyChannel.completeWithError(ErrorCategory.INSTRUCTION_EXECUTION_ERROR,
                                           "Unknown processor");
        }
    }
}
