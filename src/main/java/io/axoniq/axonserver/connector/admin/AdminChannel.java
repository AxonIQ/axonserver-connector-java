/*
 * Copyright (c) 2021. AxonIQ
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

package io.axoniq.axonserver.connector.admin;

import java.util.concurrent.CompletableFuture;

/**
 * Communication channel with AxonServer for Administration related interactions.
 *
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public interface AdminChannel {

    /**
     * Request to pause a specific event processor.
     * Returns a {@link CompletableFuture} that completes when the request has been received by AxonServer.
     * This doesn't imply that the event processor has been paused already, but only that the request has been properly
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
     * This doesn't imply that the event processor has been started already, but only that the request has been properly
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
     * This doesn't imply that the segment has been split already, but only that the request has been properly
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
     * This doesn't imply that the segments has been merged already, but only that the request has been properly
     * delivered.
     *
     * @param eventProcessorName   the name of the event processor to merge
     * @param tokenStoreIdentifier the token store identifier of the processor to merge
     * @return a {@link CompletableFuture} that completes when the request has been delivered to AxonServer
     */
    CompletableFuture<Void> mergeEventProcessor(String eventProcessorName, String tokenStoreIdentifier);
}
