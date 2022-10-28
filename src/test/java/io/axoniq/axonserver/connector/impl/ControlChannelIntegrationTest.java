/*
 * Copyright (c) 2020-2021. AxonIQ
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
import com.google.protobuf.ByteString;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.Timeout;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.command.impl.CommandChannelImpl;
import io.axoniq.axonserver.connector.control.ControlChannel;
import io.axoniq.axonserver.connector.control.ProcessorInstructionHandler;
import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.connector.query.QueryDefinition;
import io.axoniq.axonserver.connector.query.impl.QueryChannelImpl;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.grpc.control.RequestReconnect;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.apache.hc.client5.http.classic.methods.HttpPatch;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;
import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Integration test class validating the behavior of the {@link ControlChannel}.
 */
class ControlChannelIntegrationTest extends AbstractAxonServerIntegrationTest {

    private static final boolean RUNNING_PROCESSOR = true;
    private static final boolean PAUSED_PROCESSOR = false;
    private static final boolean WITHOUT_SEGMENTS = false;

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
        long endCheck = System.currentTimeMillis() + 2000;
        while (endCheck > System.currentTimeMillis()) {
            assertTrue(connection1.isConnected());
            //noinspection BusyWait
            Thread.sleep(100);
        }

        logger.info("Simulating bad connection");
        Timeout connectionIssue = axonServerProxy.toxics()
                                                 .timeout("bad_connection", ToxicDirection.DOWNSTREAM, Long.MAX_VALUE);

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
        EventProcessorInfo processorInfo = buildEventProcessorInfo(RUNNING_PROCESSOR);
        connection1.controlChannel().registerEventProcessor("testProcessor", () -> processorInfo, instructionHandler);

        assertWithin(1, TimeUnit.SECONDS, () -> {
            JsonElement response = getFromAxonServer("/v1/components/" + getClass().getSimpleName() + "/processors?context=default");
            Assertions.assertEquals("testProcessor", response.getAsJsonArray().get(0).getAsJsonObject().get("name").getAsString());
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

        //noinspection resource
        EventStream buffer = connection1.eventChannel().openStream(0, 10);

        // simulate a request to reconnect from AxonServer
        //noinspection unchecked
        ReplyChannel<PlatformInboundInstruction> mockReplyChannel = mock(ReplyChannel.class);
        PlatformOutboundInstruction instruction =
                PlatformOutboundInstruction.newBuilder()
                                           .setRequestReconnect(RequestReconnect.getDefaultInstance())
                                           .build();
        ((ControlChannelImpl) connection1.controlChannel()).handleReconnectRequest(instruction, mockReplyChannel);

        assertWithin(5, TimeUnit.SECONDS, () -> assertEquals(2, connectionCount.get()));
        assertWithin(
                1, TimeUnit.SECONDS,
                () -> assertTrue(buffer.isClosed(), "Expected Event Streams to be closed by reconnect request")
        );
    }

    @Test
    void disconnectFromContextAndReconnectWillReestablishConnection() {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection contextConnection = client.connect("default");

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(contextConnection.isReady()));

        contextConnection.disconnect();

        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(contextConnection.isConnected()));

        AxonServerConnection newContextConnection = client.connect("default");

        assertNotSame(contextConnection, newContextConnection);
        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(newContextConnection.isReady()));
    }

    @Test
    void instructionsWithoutInstructionIdAreCompletedImmediately() {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .build();
        ControlChannel controlChannel = client.connect("default")
                                              .controlChannel();

        CompletableFuture<Void> result = controlChannel.sendInstruction(PlatformInboundInstruction.getDefaultInstance());
        assertTrue(result.isDone());
    }

    @Test
    void pauseAndStartInstructionsArePickedUpByHandler() throws InterruptedException, TimeoutException {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .processorInfoUpdateFrequency(500, TimeUnit.MILLISECONDS)
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection connection = client.connect("default");
        StubProcessorInstructionHandler instructionHandler = new StubProcessorInstructionHandler();
        AtomicReference<EventProcessorInfo> processorInfo =
                new AtomicReference<>(buildEventProcessorInfo(RUNNING_PROCESSOR));

        connection.controlChannel()
                  .registerEventProcessor("testProcessor", processorInfo::get, instructionHandler)
                  .awaitAck(1, TimeUnit.SECONDS);

        String pausePath = "/v1/components/" + getClass().getSimpleName()
                + "/processors/testProcessor/pause?tokenStoreIdentifier=TokenStoreId&context=default";
        assertWithin(1, TimeUnit.SECONDS, () -> sendToAxonServer(HttpPatch::new, pausePath));
        assertWithin(5, TimeUnit.SECONDS, () -> assertTrue(instructionHandler.instructions.contains("pause")));
        processorInfo.set(buildEventProcessorInfo(PAUSED_PROCESSOR));

        String startPath = "/v1/components/" + getClass().getSimpleName()
                + "/processors/testProcessor/start?tokenStoreIdentifier=TokenStoreId&context=default";
        assertWithin(1, TimeUnit.SECONDS, () -> sendToAxonServer(HttpPatch::new, startPath));
        assertWithin(5, TimeUnit.SECONDS, () -> assertTrue(instructionHandler.instructions.contains("start")));
    }

    @Test
    void splitAndMergeInstructionsArePickedUpByHandler() throws TimeoutException, InterruptedException {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .processorInfoUpdateFrequency(500, TimeUnit.MILLISECONDS)
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection connection = client.connect("default");
        StubProcessorInstructionHandler instructionHandler = new StubProcessorInstructionHandler();
        EventProcessorInfo processorInfo = buildEventProcessorInfo(RUNNING_PROCESSOR);

        connection.controlChannel()
                   .registerEventProcessor("testProcessor", () -> processorInfo, instructionHandler)
                   .awaitAck(1, TimeUnit.SECONDS);

        String splitPath = "/v1/components/" + getClass().getSimpleName()
                + "/processors/testProcessor/segments/split?tokenStoreIdentifier=TokenStoreId&context=default";
        assertWithin(1, TimeUnit.SECONDS, () -> sendToAxonServer(HttpPatch::new, splitPath));
        // The stored instruction will split segment 1 based on the used EventProcessorInfo.
        assertTrue(instructionHandler.instructions.contains("split1"));

        String mergePath = "/v1/components/" + getClass().getSimpleName()
                + "/processors/testProcessor/segments/merge?tokenStoreIdentifier=TokenStoreId&context=default";
        assertWithin(1, TimeUnit.SECONDS, () -> sendToAxonServer(HttpPatch::new, mergePath));
        // The stored instruction will merge segment 1 based on the used EventProcessorInfo.
        assertTrue(instructionHandler.instructions.contains("merge1"));
    }

    @Test
    void moveSegmentInstructionIsPickedUpByHandler() throws Exception {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .processorInfoUpdateFrequency(500, TimeUnit.MILLISECONDS)
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection connection = client.connect("default");
        AxonServerConnectionFactory clientToMoveTo = AxonServerConnectionFactory.forClient("foo", "foo")
                                                                                .routingServers(axonServerAddress)
                                                                                .build();
        AxonServerConnection connectionToMoveTo = clientToMoveTo.connect("default");

        StubProcessorInstructionHandler instructionHandler = new StubProcessorInstructionHandler();

        connection.controlChannel()
                  .registerEventProcessor("testProcessor",
                                          () -> buildEventProcessorInfo(RUNNING_PROCESSOR),
                                          instructionHandler)
                  .awaitAck(1, TimeUnit.SECONDS);
        connectionToMoveTo.controlChannel()
                          .registerEventProcessor("testProcessor",
                                                  () -> buildEventProcessorInfo(RUNNING_PROCESSOR, WITHOUT_SEGMENTS),
                                                  instructionHandler)
                          .awaitAck(1, TimeUnit.SECONDS);

        String segmentToMove = "0";
        String segmentsPath = "/v1/components/" + getClass().getSimpleName() + "/processors/testProcessor/segments/" +
                segmentToMove + "/move?tokenStoreIdentifier=TokenStoreId&context=default&target=foo";
        assertWithin(2, TimeUnit.SECONDS, () -> sendToAxonServer(HttpPatch::new, segmentsPath));
        assertWithin(2, TimeUnit.SECONDS,
                     () -> assertTrue(instructionHandler.instructions.contains("release" + segmentToMove)));
    }

    /*
      Verifies that a connection is force-reset when an UNAVAILABLE error is returned on the Control Channel,
      while the connection itself reports to be READY. This may indicate a proxy is keeping the connection open,
      but is rejecting requests due to an unavailable backend.
     */
    @Test
    void connectionForcefullyRecreatedAfterFailureOnInstructionChannelAndLiveChannel() {
        AtomicInteger connectCounter = new AtomicInteger();
        CallCancellingInterceptor cancellingInterceptor = new CallCancellingInterceptor();
        client = AxonServerConnectionFactory.forClient("handler")
                                            .routingServers(axonServerAddress)
                                            .connectTimeout(1500, TimeUnit.MILLISECONDS)
                                            .processorInfoUpdateFrequency(500, TimeUnit.MILLISECONDS)
                                            .reconnectInterval(50, TimeUnit.MILLISECONDS)
                                            .customize(mcb -> {
                                                synchronized (this) {
                                                    connectCounter.incrementAndGet();
                                                           return mcb.intercept(cancellingInterceptor);
                                                       }
                                                   })
                                                   .build();
        AxonServerConnection connection1 = client.connect("default");

        assertEquals(1, connectCounter.get());

        QueryChannelImpl handlerClientQueryChannel = (QueryChannelImpl) connection1.queryChannel();
        CommandChannelImpl handlerClientCommandChannel = (CommandChannelImpl) connection1.commandChannel();
        ControlChannelImpl controlChannel = (ControlChannelImpl) connection1.controlChannel();

        handlerClientQueryChannel.registerQueryHandler((query, responseHandler) -> responseHandler.sendLast(QueryResponse.newBuilder().setPayload(SerializedObject.newBuilder().setData(ByteString.copyFromUtf8("Response"))).build()), new QueryDefinition("echo", String.class));
        handlerClientCommandChannel.registerCommandHandler(c -> CompletableFuture.completedFuture(CommandResponse.newBuilder().setPayload(SerializedObject.newBuilder().setData(ByteString.copyFromUtf8("Response"))).build()), 100, "echo");

        assertWithin(2, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));
        logger.info("Connection status is READY");

        assertEquals(1, connectCounter.get());

        for (int i = 0; i < 10; i++) {
            // waiting to recover
            assertWithin(2, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));
            assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(controlChannel.isReady()));

            logger.info("Simulating failing calls on successful connection");
            cancellingInterceptor.cancelAll(Status.UNAVAILABLE);
        }

        logger.info("Waiting for connector to establish connection");
        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(controlChannel.isReady()));
        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(handlerClientQueryChannel.isReady()));
        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(handlerClientCommandChannel.isReady()));

        assertEquals(11, connectCounter.get());

    }

    private static class CallCancellingInterceptor implements io.grpc.ClientInterceptor {

        private final Map<ClientCall<?, ?>, ClientCall.Listener<?>> calls = new ConcurrentHashMap<>();

        public void cancelAll(Status status) {
            calls.keySet().forEach(k -> {
                logger.debug("Causing trouble on {}", k);
                doIfNotNull(calls.remove(k), c -> c.onClose(status, null));
            });
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            ClientCall<ReqT, RespT> delegate = next.newCall(method, callOptions);
            if (!"io.axoniq.axonserver.grpc.control.PlatformService".equals(method.getServiceName()) || !"OpenStream".equals(method.getBareMethodName())) {
                return delegate;
            }
            return new ForwardingClientCall<ReqT, RespT>() {

                @Override
                protected ClientCall<ReqT, RespT> delegate() {
                    return delegate;
                }

                @Override
                public void cancel(String message, Throwable cause) {
                    calls.remove(delegate);
                    super.cancel(message, cause);
                }

                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    calls.put(delegate, responseListener);
                    super.start(responseListener, headers);
                }

                @Override
                public void halfClose() {
                    super.halfClose();
                }
            };
        }
    }

    private EventProcessorInfo buildEventProcessorInfo(boolean running) {
        return buildEventProcessorInfo(running, true);
    }

    private EventProcessorInfo buildEventProcessorInfo(boolean running, boolean withSegments) {
        EventProcessorInfo.Builder processorInfoBuilder = EventProcessorInfo.newBuilder()
                                                                            .setActiveThreads(1)
                                                                            .setAvailableThreads(4)
                                                                            .setRunning(running)
                                                                            .setMode("Tracking")
                                                                            .setProcessorName("testProcessor")
                                                                            .setTokenStoreIdentifier("TokenStoreId")
                                                                            .setIsStreamingProcessor(RUNNING_PROCESSOR);
        if (withSegments) {
            EventProcessorInfo.SegmentStatus segmentZero =
                    EventProcessorInfo.SegmentStatus.newBuilder()
                                                    .setCaughtUp(WITHOUT_SEGMENTS)
                                                    .setOnePartOf(2)
                                                    .setSegmentId(0)
                                                    .setTokenPosition(ThreadLocalRandom.current().nextInt(1, 10000))
                                                    .setReplaying(RUNNING_PROCESSOR)
                                                    .build();
            EventProcessorInfo.SegmentStatus segmentOne =
                    EventProcessorInfo.SegmentStatus.newBuilder()
                                                    .setCaughtUp(WITHOUT_SEGMENTS)
                                                    .setOnePartOf(2)
                                                    .setSegmentId(1)
                                                    .setTokenPosition(ThreadLocalRandom.current().nextInt(1, 10000))
                                                    .setReplaying(RUNNING_PROCESSOR)
                                                    .build();
            processorInfoBuilder.addSegmentStatus(segmentZero)
                                .addSegmentStatus(segmentOne);
        }
        return processorInfoBuilder.build();
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