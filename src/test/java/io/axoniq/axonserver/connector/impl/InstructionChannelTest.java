package io.axoniq.axonserver.connector.impl;

import com.google.gson.JsonElement;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.Timeout;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.instruction.ProcessorInstructionHandler;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.okhttp3.Request;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;
import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class InstructionChannelTest extends AbstractAxonServerIntegrationTest {

    private AxonServerConnectionFactory client;
    private static final Logger logger = LoggerFactory.getLogger(InstructionChannelTest.class);

    @AfterEach
    void tearDown() {
        doIfNotNull(client, AxonServerConnectionFactory::shutdown);
    }

    @Test
    void connectionRecoveredByHeartbeat() throws Exception {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .connectTimeout(2000, TimeUnit.MILLISECONDS)
                                            .reconnectInterval(500, TimeUnit.MILLISECONDS)
                                            .build();
        AxonServerConnection connection1 = client.connect("default");
        connection1.instructionChannel().enableHeartbeat(500, 500, TimeUnit.MILLISECONDS);

        assertWithin(2, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));
        logger.info("Connection status is READY");

        // we make sure the heartbeat doesn't complain at first
        long endCheck = System.currentTimeMillis() + 800;
        while (endCheck > System.currentTimeMillis()) {
            assertTrue(connection1.isConnected());
            Thread.sleep(100);
        }

        logger.info("Simulating bad connection");
        Timeout connectionIssue = axonServerProxy.toxics().timeout("bad_connection", ToxicDirection.DOWNSTREAM, Long.MAX_VALUE);

        logger.info("Waiting for connector to acknowledge broken connection");
        assertWithin(5, TimeUnit.SECONDS, () -> assertFalse(connection1.isConnected()));

        logger.info("Ending bad connection simulation");
        connectionIssue.remove();

        logger.info("Waiting for connector to establish connection");
        assertWithin(5, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));
    }

    @Test
    void testEventProcessorInformationUpdated() {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection connection1 = client.connect("default");
        ProcessorInstructionHandler instructionHandler = mock(ProcessorInstructionHandler.class);
        AtomicReference<EventProcessorInfo> processorInfo = new AtomicReference<>(buildEventProcessorInfo(true));
        connection1.instructionChannel().registerEventProcessor("testProcessor", processorInfo::get,
                                                                instructionHandler);

        assertWithin(1, TimeUnit.SECONDS, () -> {
            JsonElement response = getFromAxonServer("/v1/components/" + getClass().getSimpleName() + "/processors?context=default");
            assertEquals("testProcessor", response.getAsJsonArray().get(0).getAsJsonObject().get("name").getAsString());
        });
    }

    @Test
    void testPauseAndStartInstructionIsPickedUpByHandler() throws Exception {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection connection1 = client.connect("default");
        ProcessorInstructionHandler instructionHandler = mock(ProcessorInstructionHandler.class);
        AtomicReference<EventProcessorInfo> processorInfo = new AtomicReference<>(buildEventProcessorInfo(true));

        connection1.instructionChannel().registerEventProcessor("testProcessor", processorInfo::get,
                                                                instructionHandler);

        assertWithin(1, TimeUnit.SECONDS, () -> {
            sendToAxonServer(Request.Builder::patch, "/v1/components/" + getClass().getSimpleName() + "/processors/testProcessor/pause?context=default");
            verify(instructionHandler).pauseProcessor();
        });
        processorInfo.set(buildEventProcessorInfo(false));


        assertWithin(1, TimeUnit.SECONDS, () -> {
            sendToAxonServer(Request.Builder::patch, "/v1/components/" + getClass().getSimpleName() + "/processors/testProcessor/start?context=default");
            verify(instructionHandler).startProcessor();
        });

    }

    @Test
    void testSplitAndMergeInstructionIsPickedUpByHandler() {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection connection1 = client.connect("default");
        ProcessorInstructionHandler instructionHandler = mock(ProcessorInstructionHandler.class);
        AtomicReference<EventProcessorInfo> processorInfo = new AtomicReference<>(buildEventProcessorInfo(true));

        connection1.instructionChannel().registerEventProcessor("testProcessor", processorInfo::get,
                                                                instructionHandler);


        assertWithin(2, TimeUnit.SECONDS, () -> sendToAxonServer(Request.Builder::patch, "/v1/components/" + getClass().getSimpleName() + "/processors/testProcessor/segments/merge?context=default"));
        assertWithin(1, TimeUnit.SECONDS, () -> verify(instructionHandler).mergeSegment(eq(0)));

        assertWithin(2, TimeUnit.SECONDS, () -> sendToAxonServer(Request.Builder::patch, "/v1/components/" + getClass().getSimpleName() + "/processors/testProcessor/segments/split?context=default"));
        assertWithin(1, TimeUnit.SECONDS, () -> verify(instructionHandler).splitSegment(eq(0)));
    }

    @Test
    void testMoveSegmentInstructionIsPickedUpByHandler() {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection connection1 = client.connect("default");
        StubProcessorInstructionHandler instructionHandler = new StubProcessorInstructionHandler();
        AtomicReference<EventProcessorInfo> processorInfo = new AtomicReference<>(buildEventProcessorInfo(true));

        connection1.instructionChannel().registerEventProcessor("testProcessor", processorInfo::get,
                                                                instructionHandler);


        assertWithin(2, TimeUnit.SECONDS, () -> {
            sendToAxonServer(Request.Builder::patch, "/v1/components/" + getClass().getSimpleName() + "/processors/testProcessor/segments/0/move?context=default&target=foo");
            assertTrue(instructionHandler.instructions.contains("release0"));
        });
    }

    private EventProcessorInfo buildEventProcessorInfo(boolean running) {
        return EventProcessorInfo.newBuilder()
                                 .setActiveThreads(1)
                                 .setAvailableThreads(4)
                                 .setRunning(running)
                                 .setMode("Tracking")
                                 .setProcessorName("testProcessor")
                                 .setTokenStoreIdentifier("Unique")
                                 .addSegmentStatus(
                                         EventProcessorInfo.SegmentStatus.newBuilder()
                                                                         .setCaughtUp(false)
                                                                         .setOnePartOf(2)
                                                                         .setSegmentId(0)
                                                                         .setTokenPosition(ThreadLocalRandom.current().nextInt(1, 10000))
                                                                         .setReplaying(true)
                                                                         .build())
                                 .addSegmentStatus(
                                         EventProcessorInfo.SegmentStatus.newBuilder()
                                                                         .setCaughtUp(false)
                                                                         .setOnePartOf(2)
                                                                         .setSegmentId(1)
                                                                         .setTokenPosition(ThreadLocalRandom.current().nextInt(1, 10000))
                                                                         .setReplaying(true)
                                                                         .build())
                                 .build();

    }

    private static class StubProcessorInstructionHandler implements ProcessorInstructionHandler {

        private final List<String> instructions = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Boolean> releaseSegment(int segmentId) {
            instructions.add("release" + segmentId);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> splitSegment(int segmentId) {
            instructions.add("split" + segmentId);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> mergeSegment(int segmentId) {
            instructions.add("merge" + segmentId);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Void> pauseProcessor() {
            instructions.add("pause");
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> startProcessor() {
            instructions.add("start");
            return CompletableFuture.completedFuture(null);
        }
    }
}