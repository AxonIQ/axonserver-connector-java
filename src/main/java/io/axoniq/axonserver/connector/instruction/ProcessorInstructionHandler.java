package io.axoniq.axonserver.connector.instruction;

import java.util.concurrent.CompletableFuture;

public interface ProcessorInstructionHandler {

    CompletableFuture<Boolean> releaseSegment(int segmentId);

    CompletableFuture<Boolean> splitSegment(int segmentId);

    CompletableFuture<Boolean> mergeSegment(int segmentId);

    CompletableFuture<Void> pauseProcessor();

    CompletableFuture<Void> startProcessor();

}
