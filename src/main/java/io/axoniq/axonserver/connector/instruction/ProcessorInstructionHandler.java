package io.axoniq.axonserver.connector.instruction;

import java.util.concurrent.CompletableFuture;

/**
 * Interface describing the instructions AxonServer can send for Event Processor management.
 */
public interface ProcessorInstructionHandler {

    /**
     * Instruction to release the segment with given {@code segmentId}. If the local node doesn't have the given segment
     * claimed, it should take precautions to prevent claiming it immediately.
     * <p>
     * The return future must be completed with a {@code true} after the segment has been successfully released. When
     * failing to release the segment, the CompletableFuture must complete with either {@code false} or exceptionally.
     *
     * @param segmentId The id of the segment to release
     *
     * @return a CompletableFuture that completes to true or false, depending on the successful release of the segment
     */
    CompletableFuture<Boolean> releaseSegment(int segmentId);

    /**
     * Instruction to split the segment with given {@code segmentId}. If the local node doesn't have a claim on the
     * given segment, it must try to claim it.
     *
     * @param segmentId The identifier of the segment to split
     *
     * @return a CompletableFuture that completes to true or false, depending on the successful split of the segment
     */
    CompletableFuture<Boolean> splitSegment(int segmentId);

    /**
     * Instruction to merge the segment with given {@code segmentId} with its counterpart. If the local node doesn't
     * have a claim on the Segment with given {@code segmentId} or its counterpart, then it must attempt to claim both.
     *
     * @param segmentId The identifier of the segment to merge
     *
     * @return a CompletableFuture that completes to true or false, depending on the successful merge of the segment
     */
    CompletableFuture<Boolean> mergeSegment(int segmentId);

    /**
     * Instruction to pause the processor for which this handler was registered. All claims for Segments by the
     * processor must be released.
     *
     * @return a CompletableFuture that completes when the processor has been paused.
     */
    CompletableFuture<Void> pauseProcessor();

    /**
     * Instruction to start the processor for which this handler was registered.
     *
     * @return a CompletableFuture that completes when the processor has been started.
     */
    CompletableFuture<Void> startProcessor();

}
