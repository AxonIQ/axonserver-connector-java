package io.axoniq.axonserver.connector.control;

import java.util.concurrent.CompletableFuture;

/**
 * @author Marc Gathier
 * @since
 */
public class FakeProcessorInstructionHandler implements ProcessorInstructionHandler {

    private final CompletableFuture<Void> voidCompletableFuture = new CompletableFuture<>();
    private final CompletableFuture<Boolean> booleanCompletableFuture = new CompletableFuture<>();

    public FakeProcessorInstructionHandler() {
    }

    @Override
    public CompletableFuture<Boolean> releaseSegment(int segmentId) {
        return booleanCompletableFuture;
    }

    @Override
    public CompletableFuture<Boolean> splitSegment(int segmentId) {
        return booleanCompletableFuture;
    }

    @Override
    public CompletableFuture<Boolean> mergeSegment(int segmentId) {
        return booleanCompletableFuture;
    }

    @Override
    public CompletableFuture<Void> pauseProcessor() {
        return voidCompletableFuture;
    }

    @Override
    public CompletableFuture<Void> startProcessor() {
        return voidCompletableFuture;
    }

    public void performSuccessfully() {
        voidCompletableFuture.complete(null);
        booleanCompletableFuture.complete(true);
    }

    public void performFailing() {
        voidCompletableFuture.completeExceptionally(new RuntimeException("Failed while performing action"));
        booleanCompletableFuture.complete(false);
    }

    public void completeExceptionally(Throwable error) {
        voidCompletableFuture.completeExceptionally(error);
        booleanCompletableFuture.completeExceptionally(error);
    }
}
