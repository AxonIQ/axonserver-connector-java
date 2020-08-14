/*
 * Copyright (c) 2010-2020. Axon Framework
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
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.ResultStream;
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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class QueryChannelTest extends AbstractAxonServerIntegrationTest {

    private static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);

    private AxonServerConnectionFactory connectionFactory1;
    private AxonServerConnection connection1;
    private AxonServerConnectionFactory connectionFactory2;
    private AxonServerConnection connection2;
    private static final Logger logger = LoggerFactory.getLogger(QueryChannelTest.class);

    @BeforeEach
    void setUp() {
        connectionFactory1 = AxonServerConnectionFactory.forClient(getClass().getSimpleName() + "_Handler")
                                                        .connectTimeout(1500, TimeUnit.MILLISECONDS)
                                                        .reconnectInterval(500, TimeUnit.MILLISECONDS)
                                                        .routingServers(axonServerAddress)
                                                        .build();

        connection1 = connectionFactory1.connect("default");

        connectionFactory2 = AxonServerConnectionFactory.forClient(getClass().getSimpleName() + "_Sender")
                                                        .connectTimeout(1500, TimeUnit.MILLISECONDS)
                                                        .reconnectInterval(500, TimeUnit.MILLISECONDS)
                                                        .routingServers(axonServerAddress)
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

        ResultStream<QueryResponse> result = connection2.queryChannel().query(QueryRequest.newBuilder().setQuery("testQuery").build());

        assertWithin(2, TimeUnit.SECONDS, () -> {

            QueryResponse queryResponse = result.nextIfAvailable(100, TimeUnit.MILLISECONDS);
            assertNotNull(queryResponse);
            assertTrue(queryResponse.hasErrorMessage());
        });

        axonServerProxy.disable();

        assertWithin(100, TimeUnit.MILLISECONDS, () -> assertFalse(connection1.isReady()));

        axonServerProxy.enable();

        assertWithin(2, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

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

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(connection1.isConnectionFailed()));

        axonServerProxy.enable();

        assertWithin(3, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

        Thread.sleep(100);

        ResultStream<QueryResponse> result = connection2.queryChannel().query(QueryRequest.newBuilder().setQuery("testQuery").build());

        QueryResponse queryResponse = result.nextIfAvailable(1, TimeUnit.SECONDS);
        assertEquals("", queryResponse.getErrorMessage().getMessage());
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

        SubscriptionQueryResult subscriptionQuery = connection2.queryChannel().subscriptionQuery(QueryRequest.newBuilder().setQuery("testQuery").build(),
                                                                                                 SerializedObject.newBuilder().setType("update").build(),
                                                                                                 100, 10);

        assertWithin(1, TimeUnit.SECONDS, () ->
                assertTrue(subscriptionQuery.initialResult().isDone())
        );
        assertFalse(subscriptionQuery.initialResult().isCompletedExceptionally());
        assertWithin(1, TimeUnit.SECONDS, () -> assertNotNull(updateHandlerRef.get()));

        updateHandlerRef.get().sendUpdate(QueryUpdate.newBuilder().setPayload(SerializedObject.newBuilder().setType(String.class.getName()).setData(ByteString.copyFromUtf8("Hello")).build()).build());

        assertWithin(2, TimeUnit.SECONDS, () -> assertNotNull(subscriptionQuery.updates().nextIfAvailable()));

        axonServerProxy.disable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(connection1.isConnected()));
        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(connection2.isConnected()));

        assertWithin(1, TimeUnit.SECONDS, () -> {
            assertTrue(subscriptionQuery.updates().isClosed());
            assertNull(updateHandlerRef.get());
        });
        axonServerProxy.enable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));
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
        }, new QueryDefinition("testQuery", "testResult"));

        // we want so make sure the subscription gets a head start before we send the query for it.
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
    void testClosingSubscriptionQueryFromProviderStopsUpdateStream() throws InterruptedException {
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
        }, new QueryDefinition("testQuery", "testResult"));

        SubscriptionQueryResult subscriptionQuery = connection2.queryChannel().subscriptionQuery(QueryRequest.newBuilder()
                                                                                                             .setMessageIdentifier(subscriptionId)
                                                                                                             .setQuery("testQuery").build(),
                                                                                                 SerializedObject.newBuilder().setType("update").build(),
                                                                                                 100, 10);

        assertWithin(1, TimeUnit.SECONDS, () -> {
            assertNotNull(updateHandlerRef.get());
        });

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