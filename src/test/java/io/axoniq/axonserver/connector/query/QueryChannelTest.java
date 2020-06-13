package io.axoniq.axonserver.connector.query;

import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
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

class QueryChannelTest extends AbstractAxonServerIntegrationTest {

    private AxonServerConnectionFactory connectionFactory1;
    private AxonServerConnection connection1;
    private AxonServerConnectionFactory connectionFactory2;
    private AxonServerConnection connection2;
    private static final Logger logger = LoggerFactory.getLogger(QueryChannelTest.class);

    @BeforeEach
    void setUp() {
        connectionFactory1 = AxonServerConnectionFactory.forClient(getClass().getSimpleName() + "_Handler")
                                                        .connectTimeout(1000, TimeUnit.MILLISECONDS)
                                                        .reconnectInterval(500, TimeUnit.MILLISECONDS)
                                                        .routingServers(axonServerAddress)
                                                        .build();

        connection1 = connectionFactory1.connect("default");

        connectionFactory2 = AxonServerConnectionFactory.forClient(getClass().getSimpleName() + "_Sender")
                                                        .connectTimeout(1000, TimeUnit.MILLISECONDS)
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

        registration.cancel();

        ResultStream<QueryResponse> result = connection2.queryChannel().query(QueryRequest.newBuilder().setQuery("testQuery").build());

        assertWithin(2, TimeUnit.SECONDS, () -> {
            QueryResponse queryResponse = result.nextIfAvailable(1, TimeUnit.SECONDS);
            assertNotNull(queryResponse);
            assertTrue(queryResponse.hasErrorMessage());
        });

        axonServerProxy.disable();

        assertWithin(100, TimeUnit.MILLISECONDS, () -> assertFalse(connection1.isReady()));

        axonServerProxy.enable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

        ResultStream<QueryResponse> result2 = connection2.queryChannel().query(QueryRequest.newBuilder().setQuery("testQuery").build());
        QueryResponse actual = result2.nextIfAvailable(1, TimeUnit.SECONDS);
        assertNotNull(actual);
        assertTrue(actual.hasErrorMessage());
    }

    @Test
    void testSubscribedHandlersReconnectAfterConnectionFailure() throws Exception {
        QueryChannel queryChannel = connection1.queryChannel();
        queryChannel.registerQueryHandler(this::mockHandler, new QueryDefinition("testQuery", "testResult"));

        axonServerProxy.disable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(connection1.isConnected()));

        axonServerProxy.enable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

        Thread.sleep(100);

        ResultStream<QueryResponse> result = connection2.queryChannel().query(QueryRequest.newBuilder().setQuery("testQuery").build());

        QueryResponse queryResponse = result.nextIfAvailable(1, TimeUnit.SECONDS);
        assertFalse(queryResponse.hasErrorMessage(),
                    () -> "Unexpected message: " + queryResponse.getErrorMessage().getMessage());
    }

    @Test
    void testSubscriptionQueryCancelledOnDisconnect() throws Exception {
        connection2.instructionChannel().enableHeartbeat(100, 100, TimeUnit.MILLISECONDS);
        QueryChannel queryChannel = connection1.queryChannel();
        AtomicReference<QueryHandler.UpdateHandler> updateHandler = new AtomicReference<>();
        queryChannel.registerQueryHandler(new QueryHandler() {
            @Override
            public void handle(QueryRequest query, ResponseHandler responseHandler) {
                mockHandler(query, responseHandler);
            }

            @Override
            public Registration registerSubscriptionQuery(QueryRequest query, UpdateHandler sendUpdate) {
                updateHandler.set(sendUpdate);
                return () -> {
                    updateHandler.set(null);
                };
            }
        }, new QueryDefinition("testQuery", "testResult"));

        SubscriptionQueryResult subscriptionQuery = connection2.queryChannel().subscriptionQuery(QueryRequest.newBuilder().setQuery("testQuery").build(),
                                                                                                 SerializedObject.newBuilder().setType("update").build(),
                                                                                                 100, 10);

        assertWithin(1, TimeUnit.SECONDS, () ->
                subscriptionQuery.initialResult().isDone()
        );
        assertWithin(1, TimeUnit.SECONDS, () -> assertNotNull(updateHandler.get()));
        updateHandler.get().sendUpdate(QueryUpdate.newBuilder().build());

        assertWithin(1, TimeUnit.SECONDS, () -> assertNotNull(subscriptionQuery.updates().nextIfAvailable()));

        axonServerProxy.disable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(connection1.isConnected()));
        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(connection2.isConnected()));

        assertWithin(1, TimeUnit.SECONDS, () -> {
            assertTrue(subscriptionQuery.updates().isClosed());
            assertNull(updateHandler.get());
        });
        axonServerProxy.enable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

        Thread.sleep(100);

        assertNull(updateHandler.get());
    }

    @RepeatedTest(20)
    void testClosingSubscriptionQueryFromSenderStopsUpdateStream() throws InterruptedException, TimeoutException, ExecutionException {
        QueryChannel queryChannel = connection1.queryChannel();
        AtomicReference<QueryHandler.UpdateHandler> updateHandler = new AtomicReference<>();
        String subscriptionId = UUID.randomUUID().toString();
        queryChannel.registerQueryHandler(new QueryHandler() {
            @Override
            public void handle(QueryRequest query, ResponseHandler responseHandler) {
                logger.info("Handling query");
                mockHandler(query, responseHandler);
            }

            @Override
            public Registration registerSubscriptionQuery(QueryRequest query, UpdateHandler sendUpdate) {
                logger.info("Registering update handler for subscription query");
                if (!subscriptionId.equals(query.getMessageIdentifier())) {
                    logger.warn("Received old subscription query. Ignoring");
                    return null;
                }
                updateHandler.set(sendUpdate);
                return () -> {
                    logger.info("Clearing update handler");
                    updateHandler.set(null);
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
        assertWithin(1, TimeUnit.SECONDS, () -> assertNotNull(updateHandler.get()));
        logger.info("Sending update");
        updateHandler.get().sendUpdate(QueryUpdate.newBuilder().build());

        assertWithin(2, TimeUnit.SECONDS, () -> assertNotNull(subscriptionQuery.updates().nextIfAvailable()));

        subscriptionQuery.updates().close();

        updateHandler.get().sendUpdate(QueryUpdate.newBuilder().build());

        assertWithin(1, TimeUnit.SECONDS, () -> {
            assertNull(subscriptionQuery.updates().nextIfAvailable());
            assertTrue(subscriptionQuery.updates().isClosed(), "Client side update stream should have been closed");
            assertNull(updateHandler.get(), "Expected updateHandler to have been unregistered");
        });
    }

    @Test
    void testClosingSubscriptionQueryFromProviderStopsUpdateStream() throws InterruptedException {
        QueryChannel queryChannel = connection1.queryChannel();
        AtomicReference<QueryHandler.UpdateHandler> updateHandler = new AtomicReference<>();
        String subscriptionId = UUID.randomUUID().toString();
        queryChannel.registerQueryHandler(new QueryHandler() {
            @Override
            public void handle(QueryRequest query, ResponseHandler responseHandler) {
                mockHandler(query, responseHandler);
            }

            @Override
            public Registration registerSubscriptionQuery(QueryRequest query, UpdateHandler sendUpdate) {
                if (!subscriptionId.equals(query.getMessageIdentifier())) {
                    return null;
                }
                updateHandler.set(sendUpdate);
                return () -> {
                    updateHandler.set(null);
                };
            }
        }, new QueryDefinition("testQuery", "testResult"));

        SubscriptionQueryResult subscriptionQuery = connection2.queryChannel().subscriptionQuery(QueryRequest.newBuilder()
                                                                                                             .setMessageIdentifier(subscriptionId)
                                                                                                             .setQuery("testQuery").build(),
                                                                                                 SerializedObject.newBuilder().setType("update").build(),
                                                                                                 100, 10);

        assertWithin(1, TimeUnit.SECONDS, () -> {
            assertNotNull(updateHandler.get());
        });

        updateHandler.get().sendUpdate(QueryUpdate.newBuilder().build());
        updateHandler.get().complete();

        ResultStream<QueryUpdate> updates = subscriptionQuery.updates();
        assertWithin(1, TimeUnit.SECONDS, () -> assertNotNull(updates.nextIfAvailable()));

        assertNull(updates.nextIfAvailable());

        assertWithin(1, TimeUnit.SECONDS, () -> {
            assertTrue(updates.isClosed(), "Expected client side to be unregistered");
            assertNull(updateHandler.get(), "Expected UpdateHandler to be unregistered");
        });
    }


    private void mockHandler(QueryRequest query, QueryHandler.ResponseHandler responseHandler) {
        responseHandler.sendLastResponse(QueryResponse.newBuilder().setRequestIdentifier(query.getMessageIdentifier()).setPayload(query.getPayload()).build());
    }
}