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
