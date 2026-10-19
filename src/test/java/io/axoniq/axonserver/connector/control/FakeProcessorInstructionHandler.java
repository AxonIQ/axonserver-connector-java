/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.control;

import java.util.concurrent.CompletableFuture;

/**
 * Fake implementation of {@link ProcessorInstructionHandler} that simulates a client processing the instruction.
 *
 * @author Marc Gathier
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

    /**
     * Simulates a successful execution of the instruction.
     */
    public void performSuccessfully() {
        voidCompletableFuture.complete(null);
        booleanCompletableFuture.complete(true);
    }

    /**
     * Simulates a failed execution of the instruction.
     * The operation failed because it is not possible to perform it, the completable future is successfully completed
     * with false.
     */
    public void performFailing() {
        voidCompletableFuture.completeExceptionally(new RuntimeException("Failed while performing action"));
        booleanCompletableFuture.complete(false);
    }

    /**
     * Simulates a failed execution of the instruction.
     * The operation failed with an exception and the completable future completes exceptionally.
     *
     * @param error the exception thrown during the instruction execution.
     */
    public void completeExceptionally(Throwable error) {
        voidCompletableFuture.completeExceptionally(error);
        booleanCompletableFuture.completeExceptionally(error);
    }
}
