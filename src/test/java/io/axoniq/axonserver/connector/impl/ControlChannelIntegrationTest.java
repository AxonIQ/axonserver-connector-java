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

package io.axoniq.axonserver.connector.impl;

import com.google.gson.JsonElement;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.Timeout;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.control.ProcessorInstructionHandler;
import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.grpc.control.RequestReconnect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.okhttp3.Request;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;
import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ControlChannelIntegrationTest extends AbstractAxonServerIntegrationTest {

    private AxonServerConnectionFactory client;
    private static final Logger logger = LoggerFactory.getLogger(ControlChannelIntegrationTest.class);

    @AfterEach
    void tearDown() {
        doIfNotNull(client, AxonServerConnectionFactory::shutdown);
    }

    @Test
    void connectionRecoveredByHeartbeat() throws Exception {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .connectTimeout(1500, TimeUnit.MILLISECONDS)
                                            .processorInfoUpdateFrequency(500, TimeUnit.MILLISECONDS)
                                            .reconnectInterval(10, TimeUnit.MILLISECONDS)
                                            .build();
        AxonServerConnection connection1 = client.connect("default");
        connection1.controlChannel().enableHeartbeat(500, 500, TimeUnit.MILLISECONDS);

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
    void testEventProcessorInformationUpdated() throws TimeoutException, InterruptedException {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection connection1 = client.connect("default");
        ProcessorInstructionHandler instructionHandler = mock(ProcessorInstructionHandler.class);
        AtomicReference<EventProcessorInfo> processorInfo = new AtomicReference<>(buildEventProcessorInfo(true));
        connection1.controlChannel().registerEventProcessor("testProcessor", processorInfo::get,
                                                            instructionHandler);

        assertWithin(1, TimeUnit.SECONDS, () -> {
            JsonElement response = getFromAxonServer("/v1/components/" + getClass().getSimpleName() + "/processors?context=default");
            assertEquals("testProcessor", response.getAsJsonArray().get(0).getAsJsonObject().get("name").getAsString());
        });
    }

    @Test
    void testConnectionResetOnReconnectRequest() {
        AtomicInteger connectionCount = new AtomicInteger();
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .forceReconnectViaRoutingServers(true)
                                            .customize(b -> {
                                                connectionCount.incrementAndGet();
                                                return b;
                                            })
                                            .build();
        AxonServerConnection connection1 = client.connect("default");

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, connectionCount.get()));

        EventStream buffer = connection1.eventChannel().openStream(0, 10);

        // simulate a request to reconnect from AxonServer
        ReplyChannel<PlatformInboundInstruction> mockReplyChannel = mock(ReplyChannel.class);
        ((ControlChannelImpl) connection1.controlChannel()).handleReconnectRequest(PlatformOutboundInstruction.newBuilder().setRequestReconnect(RequestReconnect.getDefaultInstance()).build(), mockReplyChannel);

        assertWithin(5, TimeUnit.SECONDS, () -> assertEquals(2, connectionCount.get()));

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(buffer.isClosed(), "Expected Event Streams to be closed by reconnect request"));

    }

    @Test
    void testPauseAndStartInstructionIsPickedUpByHandler() throws Exception {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection connection1 = client.connect("default");
        ProcessorInstructionHandler instructionHandler = mock(ProcessorInstructionHandler.class);
        AtomicReference<EventProcessorInfo> processorInfo = new AtomicReference<>(buildEventProcessorInfo(true));

        connection1.controlChannel().registerEventProcessor("testProcessor", processorInfo::get,
                                                            instructionHandler);

        assertWithin(1, TimeUnit.SECONDS, () -> {
            sendToAxonServer(Request.Builder::patch, "/v1/components/" + getClass().getSimpleName() + "/processors/testProcessor/pause?tokenStoreIdentifier=TokenStoreId&context=default");
            verify(instructionHandler).pauseProcessor();
        });
        processorInfo.set(buildEventProcessorInfo(false));
        // these status updates are sent once per 2 seconds

        assertWithin(3, TimeUnit.SECONDS, () -> {
            sendToAxonServer(Request.Builder::patch, "/v1/components/" + getClass().getSimpleName() + "/processors/testProcessor/start?tokenStoreIdentifier=TokenStoreId&context=default");
            verify(instructionHandler).startProcessor();
        });
    }

    @Test
    void testSplitAndMergeInstructionIsPickedUpByHandler() throws TimeoutException, InterruptedException {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection connection1 = client.connect("default");
        ProcessorInstructionHandler instructionHandler = mock(ProcessorInstructionHandler.class);
        AtomicReference<EventProcessorInfo> processorInfo = new AtomicReference<>(buildEventProcessorInfo(true));

        connection1.controlChannel()
                   .registerEventProcessor("testProcessor", processorInfo::get,
                                                            instructionHandler)
                   .awaitAck(1, TimeUnit.SECONDS);

        assertWithin(2, TimeUnit.SECONDS, () -> sendToAxonServer(Request.Builder::patch, "/v1/components/" + getClass().getSimpleName() + "/processors/testProcessor/segments/merge?tokenStoreIdentifier=TokenStoreId&context=default"));
        assertWithin(1, TimeUnit.SECONDS, () -> verify(instructionHandler).mergeSegment(eq(0)));

        assertWithin(2, TimeUnit.SECONDS, () -> sendToAxonServer(Request.Builder::patch, "/v1/components/" + getClass().getSimpleName() + "/processors/testProcessor/segments/split?tokenStoreIdentifier=TokenStoreId&context=default"));
        assertWithin(1, TimeUnit.SECONDS, () -> verify(instructionHandler).splitSegment(eq(0)));
    }

    @Test
    void testMoveSegmentInstructionIsPickedUpByHandler() throws Exception {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection connection1 = client.connect("default");
        StubProcessorInstructionHandler instructionHandler = new StubProcessorInstructionHandler();
        AtomicReference<EventProcessorInfo> processorInfo = new AtomicReference<>(buildEventProcessorInfo(true));

        CountDownLatch cdl = new CountDownLatch(1);

        connection1.controlChannel()
                   .registerEventProcessor("testProcessor", () -> {
                                               cdl.countDown();
                                               return processorInfo.get();
                                           },
                                           instructionHandler)
                   .awaitAck(1, TimeUnit.SECONDS);


        // we wait for AxonServer to request data, which is an acknowledgement that the processor was registered.
        cdl.await(3, TimeUnit.SECONDS);

        sendToAxonServer(Request.Builder::patch, "/v1/components/" + getClass().getSimpleName() + "/processors/testProcessor/segments/0/move?tokenStoreIdentifier=TokenStoreId&context=default&target=foo");

        assertWithin(1, TimeUnit.SECONDS, () -> {
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
                                 .setTokenStoreIdentifier("TokenStoreId")
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