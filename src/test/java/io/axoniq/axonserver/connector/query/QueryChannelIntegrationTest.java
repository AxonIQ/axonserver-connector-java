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

package io.axoniq.axonserver.connector.query;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.impl.ContextConnection;
import io.axoniq.axonserver.connector.query.impl.QueryChannelImpl;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;
import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertFalseWithin;
import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertTrueWithin;
import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class QueryChannelIntegrationTest extends AbstractAxonServerIntegrationTest {

    private static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);
    private static final Logger logger = LoggerFactory.getLogger(QueryChannelIntegrationTest.class);
    private AxonServerConnectionFactory connectionFactory1;
    private AxonServerConnection connection1;
    private AxonServerConnectionFactory connectionFactory2;
    private AxonServerConnection connection2;

    @BeforeEach
    void setUp() {
        connectionFactory1 = AxonServerConnectionFactory.forClient(getClass().getSimpleName() + "_Handler")
                                                        .connectTimeout(1500, TimeUnit.MILLISECONDS)
                                                        .reconnectInterval(500, TimeUnit.MILLISECONDS)
                                                        .routingServers(axonServerAddress)
                                                        .queryPermits(100)
                                                        .build();

        connection1 = connectionFactory1.connect("default");

        connectionFactory2 = AxonServerConnectionFactory.forClient(getClass().getSimpleName() + "_Sender")
                                                        .connectTimeout(1500, TimeUnit.MILLISECONDS)
                                                        .reconnectInterval(500, TimeUnit.MILLISECONDS)
                                                        .routingServers(axonServerAddress)
                                                        .queryPermits(100)
                                                        .build();
        connection2 = connectionFactory2.connect("default");
    }

    @AfterEach
    void tearDown() {
        connectionFactory1.shutdown();
        connectionFactory2.shutdown();
    }

    @Test
    void testUnsubscribedHandlersDoesNotReceiveQueries() throws Exception {
        QueryChannel queryChannel = connection1.queryChannel();
        Registration registration = queryChannel.registerQueryHandler(this::mockHandler, new QueryDefinition("testQuery", "testResult"));

        registration.cancel().join();

        Thread.sleep(100); // An ACK will only confirm receipt of the instruction, not the processing of it.

        ResultStream<QueryResponse> result = connection2.queryChannel().query(QueryRequest.newBuilder().setQuery("testQuery").build());

        QueryResponse queryResponse = result.nextIfAvailable(2, TimeUnit.SECONDS);

        assertNotNull(queryResponse);

        assertTrue(queryResponse.hasErrorMessage());

        axonServerProxy.disable();

        assertFalseWithin(100, TimeUnit.MILLISECONDS, connection1::isReady);

        axonServerProxy.enable();

        assertTrueWithin(5, TimeUnit.SECONDS, connection1::isReady);
        assertTrueWithin(5, TimeUnit.SECONDS, connection2::isReady);

        ResultStream<QueryResponse> result2 = connection2.queryChannel().query(QueryRequest.newBuilder().setQuery("testQuery").build());
        QueryResponse actual = result2.nextIfAvailable(1, TimeUnit.SECONDS);
        assertNotNull(actual);
        assertTrue(actual.hasErrorMessage());
    }

    @Test
    void testSubscribedHandlersReconnectAfterConnectionFailure() throws Exception {
        QueryChannel queryChannel = connection1.queryChannel();
        queryChannel.registerQueryHandler(this::mockHandler, new QueryDefinition("testQuery", "testResult"))
                    .awaitAck(1, TimeUnit.SECONDS);

        axonServerProxy.disable();

        assertTrueWithin(1, TimeUnit.SECONDS, connection1::isConnectionFailed);

        axonServerProxy.enable();

        assertTrueWithin(3, TimeUnit.SECONDS, connection1::isReady);

        // on some environments, the subscription action may be delayed a little
        assertWithin(1500, TimeUnit.MILLISECONDS, () -> {
            ResultStream<QueryResponse> result = connection2.queryChannel().query(QueryRequest.newBuilder().setQuery("testQuery").build());
            QueryResponse queryResponse = result.nextIfAvailable(500, TimeUnit.MILLISECONDS);
            assertNotNull(queryResponse);
            assertEquals("", queryResponse.getErrorMessage().getMessage());
        });
    }

    @Test
    void testSubscriptionQueryAllowsEmptyMessageId() {
        QueryChannel queryChannel = connection1.queryChannel();
        QueryRequest queryRequest = QueryRequest.newBuilder().build();
        SerializedObject serializedObject = SerializedObject.newBuilder().build();
        assertDoesNotThrow(
                () -> queryChannel.subscriptionQuery(queryRequest, serializedObject, 5, 1));
    }

    @RepeatedTest(10)
    void testQueryChannelConsideredConnectedWhenNoHandlersSubscribed() throws IOException {
        QueryChannelImpl queryChannel = (QueryChannelImpl) connection1.queryChannel();
        // just to make sure that no attempt was made to connect, since there are no handlers
        assertTrue(queryChannel.isReady());

        // make sure no real connection can be set up
        axonServerProxy.disable();
        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(connection1.isConnected()));

        assertTrue(queryChannel.isReady());
        assertFalse(connection1.isConnected());

        Registration registration = queryChannel.registerQueryHandler((q, r) -> r.complete(),
                                                                      new QueryDefinition("test", String.class));
        AxonServerException exception = assertThrows(AxonServerException.class, () -> registration.awaitAck(1, TimeUnit.SECONDS));
        assertEquals(ErrorCategory.INSTRUCTION_ACK_ERROR, exception.getErrorCategory());

        // because of the attempt to set up a connection, it may need a few milliseconds to discover that's not possible
        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(queryChannel.isReady()));

        axonServerProxy.enable();

        // verify connection is established
        assertWithin(2, TimeUnit.SECONDS, () -> assertTrue(queryChannel.isReady()));
    }

    @Test
    void testSubscriptionQueryCancelledOnDisconnect() throws Exception {
        connection2.controlChannel().enableHeartbeat(100, 100, TimeUnit.MILLISECONDS);
        QueryChannel queryChannel = connection1.queryChannel();
        AtomicReference<QueryHandler.UpdateHandler> updateHandlerRef = new AtomicReference<>();
        queryChannel.registerQueryHandler(
                new QueryHandler() {
                    @Override
                    public void handle(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
                        mockHandler(query, responseHandler);
                    }

                    @Override
                    public Registration registerSubscriptionQuery(SubscriptionQuery query, UpdateHandler updateHandler) {
                        updateHandlerRef.set(updateHandler);
                        return () -> {
                            updateHandlerRef.set(null);
                            return COMPLETED_FUTURE;
                        };
                    }
                }, new QueryDefinition("testQuery", "testResult"))
                    .awaitAck(1, TimeUnit.SECONDS);

        SubscriptionQueryResult subscriptionQuery = connection2.queryChannel()
                                                               .subscriptionQuery(QueryRequest.newBuilder()
                                                                                              .setMessageIdentifier("id")
                                                                                              .setQuery("testQuery")
                                                                                              .build(),
                                                                                  SerializedObject.newBuilder()
                                                                                                  .setType("update")
                                                                                                  .build(),
                                                                                  100, 10);

        assertTrueWithin(1, TimeUnit.SECONDS, () -> subscriptionQuery.initialResult().isDone());
        assertFalse(subscriptionQuery.initialResult().isCompletedExceptionally());
        assertWithin(1, TimeUnit.SECONDS, () -> assertNotNull(updateHandlerRef.get()));

        updateHandlerRef.get().sendUpdate(QueryUpdate.newBuilder().setPayload(SerializedObject.newBuilder().setType(String.class.getName()).setData(ByteString.copyFromUtf8("Hello")).build()).build());

        assertWithin(2, TimeUnit.SECONDS, () -> assertNotNull(subscriptionQuery.updates().nextIfAvailable()));

        axonServerProxy.disable();

        assertFalseWithin(1, TimeUnit.SECONDS, connection1::isConnected);
        assertFalseWithin(1, TimeUnit.SECONDS, connection2::isConnected);

        assertWithin(1, TimeUnit.SECONDS, () -> {
            assertTrue(subscriptionQuery.updates().isClosed());
            assertNull(updateHandlerRef.get());
        });
        axonServerProxy.enable();

        assertTrueWithin(2, TimeUnit.SECONDS, connection1::isReady);
    }

    @RepeatedTest(20)
    void testClosingSubscriptionQueryFromSenderStopsUpdateStream() throws InterruptedException, TimeoutException, ExecutionException {
        QueryChannel queryChannel = connection1.queryChannel();
        AtomicReference<QueryHandler.UpdateHandler> updateHandlerRef = new AtomicReference<>();
        String subscriptionId = UUID.randomUUID().toString();
        queryChannel.registerQueryHandler(new QueryHandler() {
            @Override
            public void handle(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
                logger.info("Handling query");
                mockHandler(query, responseHandler);
            }

            @Override
            public Registration registerSubscriptionQuery(SubscriptionQuery query, UpdateHandler updateHandler) {
                logger.info("Registering update handler for subscription query");
                if (!subscriptionId.equals(query.getQueryRequest().getMessageIdentifier())) {
                    logger.warn("Received old subscription query. Ignoring");
                    return null;
                }
                updateHandlerRef.set(updateHandler);
                return () -> {
                    logger.info("Clearing update handler");
                    updateHandlerRef.set(null);
                    return COMPLETED_FUTURE;
                };
            }
        }, new QueryDefinition("testQuery", "testResult"))
                    .awaitAck(1, TimeUnit.SECONDS);

        Thread.sleep(100);

        SubscriptionQueryResult subscriptionQuery = connection2.queryChannel().subscriptionQuery(QueryRequest.newBuilder()
                                                                                                             .setMessageIdentifier(subscriptionId)
                                                                                                             .setQuery("testQuery")
                                                                                                             .build(),
                                                                                                 SerializedObject.newBuilder().setType("update").build(),
                                                                                                 100, 10);

        assertEquals(subscriptionId, subscriptionQuery.initialResult().get(1, TimeUnit.SECONDS)
                                                      .getRequestIdentifier());
        assertWithin(1, TimeUnit.SECONDS, () -> assertNotNull(updateHandlerRef.get()));
        logger.info("Sending update");
        updateHandlerRef.get().sendUpdate(QueryUpdate.newBuilder().build());

        assertWithin(2, TimeUnit.SECONDS, () -> assertNotNull(subscriptionQuery.updates().nextIfAvailable()));

        subscriptionQuery.updates().close();

        assertWithin(1, TimeUnit.SECONDS, () -> {
            assertNull(subscriptionQuery.updates().nextIfAvailable());
            assertTrue(subscriptionQuery.updates().isClosed(), "Client side update stream should have been closed");
            assertNull(updateHandlerRef.get(), "Expected updateHandler to have been unregistered");
        });
    }

    @Test
    void testClosingSubscriptionQueryFromProviderStopsUpdateStream() throws InterruptedException, TimeoutException {
        QueryChannel queryChannel = connection1.queryChannel();
        AtomicReference<QueryHandler.UpdateHandler> updateHandlerRef = new AtomicReference<>();
        String subscriptionId = UUID.randomUUID().toString();
        queryChannel.registerQueryHandler(new QueryHandler() {
            @Override
            public void handle(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
                mockHandler(query, responseHandler);
            }

            @Override
            public Registration registerSubscriptionQuery(SubscriptionQuery query, UpdateHandler updateHandler) {
                if (!subscriptionId.equals(query.getQueryRequest().getMessageIdentifier())) {
                    return null;
                }
                updateHandlerRef.set(updateHandler);
                return () -> {
                    updateHandlerRef.set(null);
                    return COMPLETED_FUTURE;
                };
            }
        }, new QueryDefinition("testQuery", "testResult"))
                    .awaitAck(1, TimeUnit.SECONDS);

        SubscriptionQueryResult subscriptionQuery = connection2.queryChannel().subscriptionQuery(QueryRequest.newBuilder()
                                                                                                             .setMessageIdentifier(subscriptionId)
                                                                                                             .setQuery("testQuery").build(),
                                                                                                 SerializedObject.newBuilder().setType("update").build(),
                                                                                                 100, 10);

        assertWithin(1, TimeUnit.SECONDS, () -> assertNotNull(updateHandlerRef.get()));

        updateHandlerRef.get().sendUpdate(QueryUpdate.newBuilder().build());
        updateHandlerRef.get().complete();

        ResultStream<QueryUpdate> updates = subscriptionQuery.updates();
        assertNotNull(updates.nextIfAvailable(1, TimeUnit.SECONDS));

        assertNull(updates.nextIfAvailable());

        assertWithin(1, TimeUnit.SECONDS, () -> {
            assertTrue(updates.isClosed(), "Expected client side to be unregistered");
            assertNull(updateHandlerRef.get(), "Expected UpdateHandler to be unregistered");
        });
    }

    @Test
    void testReconnectFinishesQueriesInTransit() throws InterruptedException {
        Queue<ReplyChannel<QueryResponse>> queriesInProgress = new ConcurrentLinkedQueue<>();
        QueryChannel queryChannel = connection1.queryChannel();
        queryChannel.registerQueryHandler((command, reply) -> {
            CompletableFuture<QueryResponse> result = new CompletableFuture<>();
            queriesInProgress.add(reply);
        }, new QueryDefinition("testQuery", String.class));

        QueryChannel queryChannel2 = connection2.queryChannel();

        assertWithin(1, TimeUnit.SECONDS, () -> connection1.isReady());

        Thread.sleep(100); // this is because #5 (https://github.com/AxonIQ/axonserver-connector-java/issues/5) isn't implemented yet

        List<ResultStream<QueryResponse>> actual = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            actual.add(queryChannel2.query(QueryRequest.newBuilder().setQuery("testQuery").build()));
        }

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(10, queriesInProgress.size()));

        ((ContextConnection) connection1).getManagedChannel().requestReconnect();
        ((QueryChannelImpl) connection1.queryChannel()).reconnect();

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

        while (!queriesInProgress.isEmpty()) {
            doIfNotNull(queriesInProgress.poll(), r -> r.sendLast(QueryResponse.newBuilder().setPayload(SerializedObject.newBuilder().setType("java.lang.String").setData(ByteString.copyFromUtf8("Works"))).build()));
        }

        List<QueryResponse> actualMessages = actual.stream().map(r -> {
            try {
                return r.nextIfAvailable(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                return null;
            }
        }).collect(Collectors.toList());
        actualMessages.forEach(r -> {
            assertFalse(r.hasErrorMessage());
        });

    }

    @Test
    void testDisconnectFinishesQueriesInTransit() throws InterruptedException {
        Queue<ReplyChannel<QueryResponse>> queriesInProgress = new ConcurrentLinkedQueue<>();
        QueryChannel queryChannel = connection1.queryChannel();
        queryChannel.registerQueryHandler((command, reply) -> {
            CompletableFuture<QueryResponse> result = new CompletableFuture<>();
            queriesInProgress.add(reply);
        }, new QueryDefinition("testQuery", String.class));

        QueryChannel queryChannel2 = connection2.queryChannel();

        assertWithin(1, TimeUnit.SECONDS, () -> connection1.isReady());

        Thread.sleep(100); // this is because #5 (https://github.com/AxonIQ/axonserver-connector-java/issues/5) isn't implemented yet

        List<ResultStream<QueryResponse>> actual = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            actual.add(queryChannel2.query(QueryRequest.newBuilder().setQuery("testQuery").build()));
        }

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(10, queriesInProgress.size()));

        // a call to disconnect waits for disconnection to be completed. That isn't compatible with our test here.
        CompletableFuture.runAsync(() -> connection1.disconnect());

        while (!queriesInProgress.isEmpty()) {
            doIfNotNull(queriesInProgress.poll(), r -> r.sendLast(QueryResponse.newBuilder().setPayload(SerializedObject.newBuilder().setType("java.lang.String").setData(ByteString.copyFromUtf8("Works"))).build()));
        }

        List<QueryResponse> actualMessages = actual.stream().map(r -> {
            try {
                return r.nextIfAvailable(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                return fail(e);
            }
        }).collect(Collectors.toList());
        actualMessages.forEach(r -> {
            assertFalse(r.hasErrorMessage());
        });

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(((ContextConnection) connection1).getManagedChannel().isTerminated()));
    }

    @Test
    void testUnsupportedSubscriptionQueryReturnsNoHandler() throws InterruptedException, TimeoutException {
        String subscriptionId = UUID.randomUUID().toString();
        SubscriptionQueryResult result = connection1.queryChannel().subscriptionQuery(QueryRequest.newBuilder()
                                                                                                  .setMessageIdentifier(subscriptionId)
                                                                                                  .setQuery("testQuery").build(),
                                                                                      SerializedObject.newBuilder().setType("update").build(),
                                                                                      100, 10);
        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(result.updates().isClosed()));
        try {
            result.initialResult().get(1, TimeUnit.SECONDS);
            fail("Expected an exception to be reported");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StatusRuntimeException);
            assertEquals(ErrorCategory.NO_HANDLER_FOR_QUERY.errorCode(), ((StatusRuntimeException) e.getCause()).getStatus().getDescription());
        }
    }

    @Test
    void testSubscriptionQueryReturnsNoUpdatesOnUnsupportedSubscription() throws InterruptedException, ExecutionException, TimeoutException {
        String subscriptionId = UUID.randomUUID().toString();
        connection2.queryChannel().registerQueryHandler(this::mockHandler, new QueryDefinition("testQuery", String.class))
                   .awaitAck(1, TimeUnit.SECONDS);

        SubscriptionQueryResult result = connection1.queryChannel().subscriptionQuery(QueryRequest.newBuilder()
                                                                                                  .setMessageIdentifier(subscriptionId)
                                                                                                  .setQuery("testQuery").build(),
                                                                                      SerializedObject.newBuilder().setType("update").build(),
                                                                                      100, 10);

        result.initialResult().get(1, TimeUnit.SECONDS);
        assertNull(result.updates().nextIfAvailable());
        assertFalse(result.updates().isClosed());

        result.updates().close();

        assertTrue(result.updates().isClosed());
    }

    private void mockHandler(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
        responseHandler.sendLast(QueryResponse.newBuilder().setRequestIdentifier(query.getMessageIdentifier()).setPayload(query.getPayload()).build());
    }
}