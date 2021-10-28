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
}
