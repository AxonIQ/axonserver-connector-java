package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ErrorCode;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.instruction.ProcessorInstructionHandler;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.axoniq.axonserver.connector.impl.MessageFactory.buildErrorMessage;

public class ProcessorInstructions {

    private ProcessorInstructions() {
    }

    public static BiConsumer<PlatformOutboundInstruction, ReplyChannel<PlatformInboundInstruction>> mergeHandler(Map<String, ProcessorInstructionHandler> instructionHandlers) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getMergeEventProcessorSegment().getProcessorName();
            int segmentId = instruction.getMergeEventProcessorSegment().getSegmentIdentifier();
            executeAndReply(replyChannel,
                            instructionHandlers.get(processorName),
                            h -> h.mergeSegment(segmentId));
        };
    }

    public static BiConsumer<PlatformOutboundInstruction, ReplyChannel<PlatformInboundInstruction>> splitHandler(Map<String, ProcessorInstructionHandler> instructionHandlers) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getSplitEventProcessorSegment().getProcessorName();
            int segmentId = instruction.getSplitEventProcessorSegment().getSegmentIdentifier();
            executeAndReply(replyChannel,
                            instructionHandlers.get(processorName),
                            h -> h.splitSegment(segmentId));
        };

    }

    public static BiConsumer<PlatformOutboundInstruction, ReplyChannel<PlatformInboundInstruction>> startHandler(Map<String, ProcessorInstructionHandler> instructionHandlers) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getStartEventProcessor().getProcessorName();
            executeAndReply(replyChannel,
                            instructionHandlers.get(processorName),
                            h -> h.startProcessor().thenApply(r -> true));
        };
    }

    public static BiConsumer<PlatformOutboundInstruction, ReplyChannel<PlatformInboundInstruction>> pauseHandler(Map<String, ProcessorInstructionHandler> instructionHandlers) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getPauseEventProcessor().getProcessorName();
            executeAndReply(replyChannel,
                            instructionHandlers.get(processorName),
                            h -> h.pauseProcessor().thenApply(r -> true));
        };
    }

    public static BiConsumer<PlatformOutboundInstruction, ReplyChannel<PlatformInboundInstruction>> releaseSegmentHandler(Map<String, ProcessorInstructionHandler> instructionHandlers) {
        return (instruction, replyChannel) -> {
            String processorName = instruction.getReleaseSegment().getProcessorName();
            int segmentId = instruction.getReleaseSegment().getSegmentIdentifier();

            executeAndReply(replyChannel,
                            instructionHandlers.get(processorName),
                            h -> h.releaseSegment(segmentId));
        };
    }

    public static BiConsumer<PlatformOutboundInstruction, ReplyChannel<PlatformInboundInstruction>> requestInfoHandler(Map<String, Supplier<EventProcessorInfo>> infoSuppliers) {
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
                    boolean success = r == null ? false : r;
                    if (!success || e != null) {
                        replyChannel.completeWithError(buildErrorMessage(ErrorCode.INSTRUCTION_EXECUTION_ERROR, "client", e));
                    } else {
                        replyChannel.complete();
                    }

                });
        } else {
            replyChannel.completeWithError(ErrorCode.INSTRUCTION_EXECUTION_ERROR,
                                           "Unknown processor");
        }
    }
}
