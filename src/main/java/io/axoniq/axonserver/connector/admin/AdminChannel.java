package io.axoniq.axonserver.connector.admin;

import java.util.concurrent.CompletableFuture;

/**
 * Communication channel with AxonServer for Administration related interactions.
 *
 * @author Sara Pellegrini
 * @since 4.6
 */
public interface AdminChannel {

    /**
     * Request to pause a specific event processor.
     * Returns a {@link CompletableFuture} that completes when the request has been received by AxonServer.
     * This doesn't imply that the event processor has been already paused, but only that the request has been properly
     * delivered.
     *
     * @param eventProcessorName   the name of the event processor to pause
     * @param tokenStoreIdentifier the token store identifier of the processor to pause
     * @return a {@link CompletableFuture} that completes when the request has been delivered to AxonServer
     */
    CompletableFuture<Void> pauseEventProcessor(String eventProcessorName, String tokenStoreIdentifier);

    /**
     * Request to start a specific event processor.
     * Returns a {@link CompletableFuture} that completes when the request has been received by AxonServer.
     * This doesn't imply that the event processor has been already started, but only that the request has been properly
     * delivered.
     *
     * @param eventProcessorName   the name of the event processor to start
     * @param tokenStoreIdentifier the token store identifier of the processor to start
     * @return a {@link CompletableFuture} that completes when the request has been delivered to AxonServer
     */
    CompletableFuture<Void> startEventProcessor(String eventProcessorName, String tokenStoreIdentifier);

    /**
     * Request to split the biggest segment of a specific event processor.
     * Returns a {@link CompletableFuture} that completes when the request has been received by AxonServer.
     * This doesn't imply that the segment has been already split, but only that the request has been properly
     * delivered.
     *
     * @param eventProcessorName   the name of the event processor to split
     * @param tokenStoreIdentifier the token store identifier of the processor to split
     * @return a {@link CompletableFuture} that completes when the request has been delivered to AxonServer
     */
    CompletableFuture<Void> splitEventProcessor(String eventProcessorName, String tokenStoreIdentifier);

    /**
     * Request to merge the two smallest segments of a specific event processor.
     * Returns a {@link CompletableFuture} that completes when the request has been received by AxonServer.
     * This doesn't imply that the segments has been already merged, but only that the request has been properly
     * delivered.
     *
     * @param eventProcessorName   the name of the event processor to merge
     * @param tokenStoreIdentifier the token store identifier of the processor to merge
     * @return a {@link CompletableFuture} that completes when the request has been delivered to AxonServer
     */
    CompletableFuture<Void> mergeEventProcessor(String eventProcessorName, String tokenStoreIdentifier);
}
