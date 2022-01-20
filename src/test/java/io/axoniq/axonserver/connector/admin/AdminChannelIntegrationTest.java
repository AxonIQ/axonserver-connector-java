/*
 * Copyright (c) 2022. AxonIQ
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

import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.ResultStreamPublisher;
import io.axoniq.axonserver.connector.control.ProcessorInstructionHandler;
import io.axoniq.axonserver.grpc.admin.EventProcessor;
import io.axoniq.axonserver.grpc.admin.EventProcessorInstance;
import io.axoniq.axonserver.grpc.admin.EventProcessorSegment;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo.SegmentStatus;
import org.junit.jupiter.api.*;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for {@link io.axoniq.axonserver.connector.admin.impl.AdminChannelImpl}.
 *
 * @author Sara Pellegrini
 * @since 4.6.0
 */
class AdminChannelIntegrationTest extends AbstractAxonServerIntegrationTest {

    private final String processorName = "eventProcessor";
    private final String tokenStoreIdentifier = "myTokenStore";
    private final String mode = "Tracking";
    private AxonServerConnectionFactory client;
    private AxonServerConnection connection;

    @BeforeEach
    void setUp() {
        client = AxonServerConnectionFactory.forClient("admin-client", "admin-client-1")
                                            .routingServers(axonServerAddress)
                                            .reconnectInterval(500, MILLISECONDS)
                                            .build();
        connection = client.connect("default");
    }

    @AfterEach
    void tearDown() {
        client.shutdown();
    }

    @Test
    @Disabled("To reactivate after the release of AS 4.6.0")
    void testStreamEventProcessorState() throws Exception {
        Supplier<EventProcessorInfo> eventProcessorInfoSupplier = this::eventProcessorInfo;
        ProcessorInstructionHandler handler = mock(ProcessorInstructionHandler.class);
        connection.controlChannel().registerEventProcessor(processorName, eventProcessorInfoSupplier, handler);
        ResultStream<EventProcessor> resultStream = connection.adminChannel().eventProcessors();
        assertEventProcessorInfo(resultStream.next());
        assertTrue(resultStream.isClosed());
    }

    @Test
    @Disabled("To reactivate after the release of AS 4.6.0")
    void testStreamEventProcessorStateByApplication() throws Exception {
        Supplier<EventProcessorInfo> eventProcessorInfoSupplier = this::eventProcessorInfo;
        ProcessorInstructionHandler handler = mock(ProcessorInstructionHandler.class);
        connection.controlChannel().registerEventProcessor(processorName, eventProcessorInfoSupplier, handler);
        ResultStream<EventProcessor> resultStream = connection.adminChannel().eventProcessorsByApplication(
                "admin-client");
        assertEventProcessorInfo(resultStream.next());
        assertTrue(resultStream.isClosed());
        ResultStream<EventProcessor> emptyResult = connection.adminChannel()
                                                             .eventProcessorsByApplication("another-app");
        assertNull(emptyResult.nextIfAvailable(100, MILLISECONDS));
        assertTrue(emptyResult.isClosed());
    }

    @Test
    @Disabled("To reactivate after the release of AS 4.6.0")
    void testStreamEventProcessorStateAsPublisher() {
        Supplier<EventProcessorInfo> eventProcessorInfoSupplier = this::eventProcessorInfo;
        ProcessorInstructionHandler handler = mock(ProcessorInstructionHandler.class);
        connection.controlChannel().registerEventProcessor(processorName, eventProcessorInfoSupplier, handler);

        Publisher<EventProcessor> publisher = new ResultStreamPublisher<>(
                () -> connection.adminChannel().eventProcessors()
        );

        StepVerifier.create(publisher)
                    .expectNextMatches(this::assertEventProcessorInfo)
                    .verifyComplete();
    }

    private EventProcessorInfo eventProcessorInfo() {
        return EventProcessorInfo.newBuilder()
                                 .setProcessorName(processorName)
                                 .setTokenStoreIdentifier(tokenStoreIdentifier)
                                 .setRunning(true)
                                 .setActiveThreads(1)
                                 .setAvailableThreads(10)
                                 .setMode(mode)
                                 .setError(true)
                                 .setIsStreamingProcessor(true)
                                 .addSegmentStatus(SegmentStatus.newBuilder()
                                                                .setSegmentId(0)
                                                                .setTokenPosition(100)
                                                                .setOnePartOf(1)
                                                                .setCaughtUp(true)
                                                                .setReplaying(true)
                                                                .setErrorState("error")
                                                                .build())
                                 .build();
    }

    private boolean assertEventProcessorInfo(EventProcessor eventProcessor) {
        assertEquals(processorName, eventProcessor.getIdentifier().getProcessorName());
        assertEquals(tokenStoreIdentifier, eventProcessor.getIdentifier().getTokenStoreIdentifier());
        assertEquals(mode, eventProcessor.getMode());
        assertTrue(eventProcessor.getIsStreaming());
        assertEquals(1, eventProcessor.getClientInstanceCount());
        EventProcessorInstance instance = eventProcessor.getClientInstanceList().get(0);
        assertEquals("admin-client-1", instance.getClientId());
        assertTrue(instance.getIsRunning());
        assertEquals(11, instance.getMaxSegments());
        assertEquals(1, instance.getClaimedSegmentCount());
        EventProcessorSegment segment = instance.getClaimedSegmentList().get(0);
        assertEquals(1, segment.getOnePartOf());
        assertEquals("error", segment.getError());
        assertTrue(segment.getIsCaughtUp());
        assertTrue(segment.getIsReplaying());
        assertTrue(segment.getIsInError());
        assertEquals("admin-client-1", segment.getClaimedBy());
        return true;
    }


    @Test
    @Disabled("To reactivate after the release of AS 4.6.0")
    void testPauseEventProcessor() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        CompletableFuture<Void> accepted = adminChannel.pauseEventProcessor("processor", "tokenStore");
        accepted.get(1, SECONDS);
        Assertions.assertTrue(accepted.isDone());
    }

    @Test
    @Disabled("To reactivate after the release of AS 4.6.0")
    void testStartEventProcessor() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        CompletableFuture<Void> accepted = adminChannel.startEventProcessor("processor", "tokenStore");
        accepted.get(1, SECONDS);
        Assertions.assertTrue(accepted.isDone());
    }

    @Test
    @Disabled("To reactivate after the release of AS 4.6.0")
    void testSplitEventProcessor() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        CompletableFuture<Void> accepted = adminChannel.splitEventProcessor("processor", "tokenStore");
        accepted.get(1, SECONDS);
        Assertions.assertTrue(accepted.isDone());
    }

    @Test
    @Disabled("To reactivate after the release of AS 4.6.0")
    void testMergeEventProcessor() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        CompletableFuture<Void> accepted = adminChannel.mergeEventProcessor("processor", "tokenStore");
        accepted.get(1, SECONDS);
        Assertions.assertTrue(accepted.isDone());
    }
}
