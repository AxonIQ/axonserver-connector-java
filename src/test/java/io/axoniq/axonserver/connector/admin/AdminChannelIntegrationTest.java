/*
 * Copyright (c) 2020-2022. AxonIQ
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
import io.axoniq.axonserver.connector.ResultStreamPublisher;
import io.axoniq.axonserver.connector.control.FakeProcessorInstructionHandler;
import io.axoniq.axonserver.connector.control.ProcessorInstructionHandler;
import io.axoniq.axonserver.grpc.admin.EventProcessor;
import io.axoniq.axonserver.grpc.admin.EventProcessorInstance;
import io.axoniq.axonserver.grpc.admin.EventProcessorSegment;
import io.axoniq.axonserver.grpc.admin.Result;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo.SegmentStatus;
import io.grpc.Status;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertFor;
import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Integration tests for {@link io.axoniq.axonserver.connector.admin.impl.AdminChannelImpl}.
 *
 * @author Sara Pellegrini
 * @since 4.6.0
 */
@Disabled("To reactivate after the release of AS 4.6.0")
class AdminChannelIntegrationTest  extends AbstractAxonServerIntegrationTest {

    private final String processorName = "eventProcessor";
    private final String tokenStoreIdentifier = "myTokenStore";
    private final String mode = "tracking";
    private final String adminClientId = "admin-client-1";
    private AxonServerConnectionFactory client;
    private AxonServerConnection connection;
    private final String componentName = "admin-client";

    @BeforeEach
    void setUp() {
        client = AxonServerConnectionFactory.forClient(componentName, adminClientId)
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
    void testStreamEventProcessorState() {
        Supplier<EventProcessorInfo> eventProcessorInfoSupplier = this::eventProcessorInfo;
        ProcessorInstructionHandler handler = mock(ProcessorInstructionHandler.class);
        connection.controlChannel().registerEventProcessor(processorName, eventProcessorInfoSupplier, handler);

        ResultStreamPublisher<EventProcessor> streamPublisher = new ResultStreamPublisher<>(() -> connection.adminChannel()
                                                                                                            .eventProcessors());
        StepVerifier.create(streamPublisher)
                    .expectNextMatches(this::assertEventProcessorInfo).verifyComplete();
    }

    @Test
    void testStreamEventProcessorStateByApplication() {
        Supplier<EventProcessorInfo> eventProcessorInfoSupplier = this::eventProcessorInfo;
        ProcessorInstructionHandler handler = mock(ProcessorInstructionHandler.class);
        connection.controlChannel().registerEventProcessor(processorName, eventProcessorInfoSupplier, handler);

        ResultStreamPublisher<EventProcessor> streamPublisher = new ResultStreamPublisher<>(() -> connection.adminChannel()
                                                                                                            .eventProcessorsByComponent(
                                                                                                                    "admin-client"));
        StepVerifier.create(streamPublisher)
                    .expectNextMatches(this::assertEventProcessorInfo).verifyComplete();


        ResultStreamPublisher<EventProcessor> anotherPublisher = new ResultStreamPublisher<>(() -> connection.adminChannel()
                                                                                                             .eventProcessorsByComponent(
                                                                                                                     "another-client"));
        StepVerifier.create(anotherPublisher)
                    .verifyComplete();
    }

    @Test
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
                                 .setActiveThreads(2)
                                 .setAvailableThreads(10)
                                 .setMode(mode)
                                 .setError(true)
                                 .setIsStreamingProcessor(true)
                                 .addSegmentStatus(SegmentStatus.newBuilder()
                                                                .setSegmentId(0)
                                                                .setTokenPosition(100)
                                                                .setOnePartOf(2)
                                                                .setCaughtUp(true)
                                                                .setReplaying(true)
                                                                .setErrorState("error")
                                                                .build())
                                 .addSegmentStatus(SegmentStatus.newBuilder()
                                                                .setSegmentId(1)
                                                                .setTokenPosition(100)
                                                                .setOnePartOf(2)
                                                                .setCaughtUp(true)
                                                                .setReplaying(true)
                                                                .setErrorState("error")
                                                                .build())
                                 .build();
    }

    private EventProcessorInfo anotherEventProcessorInfo() {
        return EventProcessorInfo.newBuilder()
                                 .setProcessorName(processorName)
                                 .setTokenStoreIdentifier(tokenStoreIdentifier)
                                 .setRunning(true)
                                 .setAvailableThreads(10)
                                 .setMode(mode)
                                 .setError(true)
                                 .setIsStreamingProcessor(true)
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
        assertEquals(12, instance.getMaxCapacity());
        assertEquals(2, instance.getClaimedSegmentCount());
        EventProcessorSegment segment = instance.getClaimedSegmentList().get(0);
        assertEquals(2, segment.getOnePartOf());
        assertEquals("error", segment.getError());
        assertTrue(segment.getIsCaughtUp());
        assertTrue(segment.getIsReplaying());
        assertTrue(segment.getIsInError());
        assertEquals("admin-client-1", segment.getClaimedBy());
        segment = instance.getClaimedSegmentList().get(1);
        assertEquals(2, segment.getOnePartOf());
        assertEquals("error", segment.getError());
        assertTrue(segment.getIsCaughtUp());
        assertTrue(segment.getIsReplaying());
        assertTrue(segment.getIsInError());
        assertEquals("admin-client-1", segment.getClaimedBy());
        return true;
    }


    @Test
    void testPauseEventProcessor() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        FakeProcessorInstructionHandler processorInstructionHandler = registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.pauseEventProcessor(processorName, tokenStoreIdentifier);
        checkHandleSuccess(accepted, processorInstructionHandler);
    }

    @Test
    void testPauseEventProcessorFailing() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        FakeProcessorInstructionHandler processorInstructionHandler = registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.pauseEventProcessor(processorName, tokenStoreIdentifier);
        assertFor(Duration.ofMillis(500), Duration.ofMillis(100), () -> assertFalse(accepted.isDone()));
        processorInstructionHandler.performFailing();
        expectException(accepted, Status.Code.CANCELLED);
    }


    @Test
    void testPauseEventProcessorTimeout() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.pauseEventProcessor(processorName, tokenStoreIdentifier);
        expectException(accepted, 10, Status.Code.DEADLINE_EXCEEDED);
    }

    @Test
    void testStartEventProcessor() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        FakeProcessorInstructionHandler processorInstructionHandler = registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.startEventProcessor(processorName, tokenStoreIdentifier);
        checkHandleSuccess(accepted, processorInstructionHandler);
    }

    @Test
    void testStartEventProcessorFailing() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        FakeProcessorInstructionHandler processorInstructionHandler = registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.startEventProcessor(processorName, tokenStoreIdentifier);
        assertFor(Duration.ofMillis(500), Duration.ofMillis(100), () -> assertFalse(accepted.isDone()));
        processorInstructionHandler.performFailing();
        expectException(accepted, Status.Code.CANCELLED);
    }


    @Test
    void testStartEventProcessorTimeout() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.startEventProcessor(processorName, tokenStoreIdentifier);
        expectException(accepted, 10, Status.Code.DEADLINE_EXCEEDED);
    }

    @Test
    void testSplitEventProcessor() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        FakeProcessorInstructionHandler processorInstructionHandler = registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.splitEventProcessor(processorName, tokenStoreIdentifier);
        checkHandleSuccess(accepted, processorInstructionHandler);
    }

    @Test
    void testSplitEventProcessorFailing() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        FakeProcessorInstructionHandler processorInstructionHandler = registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.splitEventProcessor(processorName, tokenStoreIdentifier);
        assertFor(Duration.ofMillis(500), Duration.ofMillis(100), () -> assertFalse(accepted.isDone()));
        processorInstructionHandler.performFailing();
        expectException(accepted, Status.Code.CANCELLED);
    }

    @Test
    void testSplitEventProcessorCompletedExceptionally() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        FakeProcessorInstructionHandler processorInstructionHandler = registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.splitEventProcessor(processorName, tokenStoreIdentifier);
        assertFor(Duration.ofMillis(500), Duration.ofMillis(100), () -> assertFalse(accepted.isDone()));
        processorInstructionHandler.completeExceptionally(new RuntimeException("something failed"));
        expectException(accepted, Status.Code.CANCELLED);
    }

    @NotNull
    private FakeProcessorInstructionHandler registerEventProcessor() {
        FakeProcessorInstructionHandler processorInstructionHandler = new FakeProcessorInstructionHandler();
        connection.controlChannel().registerEventProcessor(processorName,
                                                           this::eventProcessorInfo,
                                                           processorInstructionHandler);
        waitUntilEventProcessorExists(processorName, adminClientId);
        return processorInstructionHandler;
    }

    @Test
    void testSplitEventProcessorTimeout() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.splitEventProcessor(processorName, tokenStoreIdentifier);
        expectException(accepted, 10, Status.Code.DEADLINE_EXCEEDED);
    }

    @Test
    void testMergeEventProcessor() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        FakeProcessorInstructionHandler processorInstructionHandler = registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.mergeEventProcessor(processorName, tokenStoreIdentifier);
        checkHandleSuccess(accepted, processorInstructionHandler);
    }

    @Test
    void testMergeEventProcessorFailing() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        FakeProcessorInstructionHandler processorInstructionHandler = registerEventProcessor();

        connection.controlChannel().registerEventProcessor(processorName,
                                                           this::eventProcessorInfo,
                                                           processorInstructionHandler);
        waitUntilEventProcessorExists(processorName, adminClientId);
        CompletableFuture<Result> accepted = adminChannel.mergeEventProcessor(processorName, tokenStoreIdentifier);
        assertFor(Duration.ofMillis(500), Duration.ofMillis(100), () -> assertFalse(accepted.isDone()));
        processorInstructionHandler.performFailing();
        expectException(accepted, Status.Code.CANCELLED);
    }

    @Test
    void testMergeEventProcessorTimeout() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.mergeEventProcessor(processorName, tokenStoreIdentifier);
        expectException(accepted, 10, Status.Code.DEADLINE_EXCEEDED);
    }

    @Test
    void testMergeEventProcessorCompletedExceptionally() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        FakeProcessorInstructionHandler processorInstructionHandler = registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.mergeEventProcessor(processorName, tokenStoreIdentifier);
        assertFor(Duration.ofMillis(500), Duration.ofMillis(100), () -> assertFalse(accepted.isDone()));
        processorInstructionHandler.completeExceptionally(new RuntimeException("something failed"));
        expectException(accepted, Status.Code.CANCELLED);
    }

    @Test
    void testMoveEventProcessorSegmentWhenAlreadyClaimed() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        registerEventProcessor();
        CompletableFuture<Result> accepted = adminChannel.moveEventProcessorSegment(processorName,
                                                                                    tokenStoreIdentifier,
                                                                                    0,
                                                                                    adminClientId);
        accepted.get(1, SECONDS);
        Assertions.assertTrue(accepted.isDone());
    }


    @Test
    void testMoveEventProcessorSegment() throws Exception {
        AxonServerConnectionFactory anotherClient = AxonServerConnectionFactory.forClient("admin-client-2",
                                                                                          "another-client-id")
                                                                               .routingServers(axonServerAddress)
                                                                               .reconnectInterval(500, MILLISECONDS)
                                                                               .build();
        try {
            AxonServerConnection anotherConnection = anotherClient.connect("default");
            AdminChannel adminChannel = connection.adminChannel();
            FakeProcessorInstructionHandler processorInstructionHandler = registerEventProcessor();
            anotherConnection.controlChannel().registerEventProcessor(processorName,
                                                                      this::anotherEventProcessorInfo,
                                                                      new FakeProcessorInstructionHandler());
            waitUntilEventProcessorExists(processorName, "another-client-id");
            CompletableFuture<Result> accepted = adminChannel.moveEventProcessorSegment(processorName,
                                                                                        tokenStoreIdentifier,
                                                                                        0,
                                                                                        "another-client-id");
            checkHandleSuccess(accepted, processorInstructionHandler);
        } finally {
            anotherClient.shutdown();
        }
    }

    @Test
    void testMoveEventProcessorSegmentTimeout() throws Exception {
        AxonServerConnectionFactory anotherClient = AxonServerConnectionFactory.forClient("admin-client-2",
                                                                                          "another-client-id")
                                                                               .routingServers(axonServerAddress)
                                                                               .reconnectInterval(500, MILLISECONDS)
                                                                               .build();
        try {
            AxonServerConnection anotherConnection = anotherClient.connect("default");
            AdminChannel adminChannel = connection.adminChannel();
            registerEventProcessor();
            anotherConnection.controlChannel().registerEventProcessor(processorName,
                                                                      this::anotherEventProcessorInfo,
                                                                      new FakeProcessorInstructionHandler());
            waitUntilEventProcessorExists(processorName, "another-client-id");
            CompletableFuture<Result> accepted = adminChannel.moveEventProcessorSegment(processorName,
                                                                                        tokenStoreIdentifier,
                                                                                        0,
                                                                                        "another-client-id");
            expectException(accepted, 10, Status.Code.DEADLINE_EXCEEDED);
        } finally {
            anotherClient.shutdown();
        }
    }

    @Test
    void testMoveEventProcessorSegmentWhenTargetMissing() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        FakeProcessorInstructionHandler processorInstructionHandler = registerEventProcessor();
        connection.controlChannel().registerEventProcessor(processorName,
                                                           this::eventProcessorInfo,
                                                           processorInstructionHandler);
        CompletableFuture<Result> accepted = adminChannel.moveEventProcessorSegment(processorName,
                                                                                    tokenStoreIdentifier,
                                                                                    0,
                                                                                    "anotherClient");
        expectException(accepted, Status.Code.NOT_FOUND);
    }

    private void expectException(CompletableFuture<?> accepted, Status.Code expected)
            throws InterruptedException, TimeoutException {
        expectException(accepted, 1, expected);
    }

    private void expectException(CompletableFuture<?> accepted, int timeout, Status.Code expected)
            throws InterruptedException, TimeoutException {
        try {
            accepted.get(timeout, SECONDS);
            fail("Execution should throw an exception");
        } catch (ExecutionException executionException) {
            assertEquals(expected, Status.fromThrowable(executionException).getCode());
        }
    }

    private void checkHandleSuccess(CompletableFuture<?> accepted,
                                    FakeProcessorInstructionHandler processorInstructionHandler)
            throws InterruptedException, ExecutionException, TimeoutException {
        assertFor(Duration.ofMillis(500), Duration.ofMillis(100), () -> assertFalse(accepted.isDone()));
        processorInstructionHandler.performSuccessfully();
        accepted.get(1, SECONDS);
        Assertions.assertTrue(accepted.isDone());
    }


    @Test
    @Disabled("To reactivate after the release of AS 4.6.0")
    void loadBalance() throws Exception {
        Supplier<EventProcessorInfo> eventProcessorInfoSupplier = this::eventProcessorInfo;
        ProcessorInstructionHandler handler = mock(ProcessorInstructionHandler.class);
        connection.controlChannel().registerEventProcessor(processorName, eventProcessorInfoSupplier, handler);
        AdminChannel adminChannel = connection.adminChannel();
        CompletableFuture<Void> accepted = adminChannel.getBalancingStrategies()
                .thenCompose(balancingStrategies -> adminChannel.loadBalanceEventProcessor("processor", "tokenStore", balancingStrategies.get(0).getStrategy()));

        accepted.get(1, SECONDS);
        Assertions.assertTrue(accepted.isDone());
    }

    @Test
    @Disabled("To reactivate after the release of AS 4.6.0")
    void autoLoadBalance() throws Exception {
        Supplier<EventProcessorInfo> eventProcessorInfoSupplier = this::eventProcessorInfo;
        ProcessorInstructionHandler handler = mock(ProcessorInstructionHandler.class);
        connection.controlChannel().registerEventProcessor(processorName, eventProcessorInfoSupplier, handler);
        AdminChannel adminChannel = connection.adminChannel();
        CompletableFuture<Void> accepted = adminChannel.getBalancingStrategies()
                                                       .thenCompose(balancingStrategies -> adminChannel.setAutoLoadBalanceStrategy("processor", "tokenStore", balancingStrategies.get(1).getStrategy()));
        accepted.get(1, SECONDS);
        Assertions.assertTrue(accepted.isDone());
    }

    private void waitUntilEventProcessorExists(String processorName, String clientId) {
        assertWithin(1, SECONDS, () -> {
            ResultStreamPublisher<EventProcessor> publisher = new ResultStreamPublisher<>(() -> connection.adminChannel()
                                                                                                          .eventProcessorsByComponent(
                                                                                                                  componentName));
            assertEquals(Boolean.TRUE, Flux.from(publisher)
                                           .any(ep -> ep.getIdentifier().getProcessorName().equals(processorName)
                                                   && hasClient(ep.getClientInstanceList(), clientId))
                                           .block());
        });
    }

    private boolean hasClient(List<EventProcessorInstance> clientInstanceList, String clientId) {
        return clientInstanceList.stream().anyMatch(epi -> epi.getClientId().equals(clientId));
    }
}
