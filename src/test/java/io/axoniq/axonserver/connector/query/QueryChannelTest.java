package io.axoniq.axonserver.connector.query;

import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryChannelTest extends AbstractAxonServerIntegrationTest {

    private AxonServerConnectionFactory connectionFactory1;
    private AxonServerConnection connection1;
    private AxonServerConnectionFactory connectionFactory2;
    private AxonServerConnection connection2;

    @BeforeEach
    void setUp() {
        connectionFactory1 = AxonServerConnectionFactory.forClient(getClass().getSimpleName() + "_Handler")
                                                        .routingServers(axonServerAddress);

        connection1 = connectionFactory1.connect("default");

        connectionFactory2 = AxonServerConnectionFactory.forClient(getClass().getSimpleName() + "_Sender")
                                                        .routingServers(axonServerAddress);
        connection2 = connectionFactory2.connect("default");
    }

    @AfterEach
    void tearDown() {
        connectionFactory1.shutdown();
        connectionFactory2.shutdown();
    }

    @Test
    void testUnsubscribedHandlersDoesNotReceiveQuerys() throws Exception {
        QueryChannel queryChannel = connection1.queryChannel();
        Registration registration = queryChannel.registerQueryHandler(this::mockHandler, new QueryDefinition("testQuery", "testResult"));

        registration.cancel();

        ResultStream<QueryResponse> result = connection2.queryChannel().query(QueryRequest.newBuilder().setQuery("testQuery").build());

        assertTrue(result.nextIfAvailable(1, TimeUnit.SECONDS).hasErrorMessage());

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

        ResultStream<QueryResponse> result = connection2.queryChannel().query(QueryRequest.newBuilder().setQuery("testQuery").build());

        QueryResponse queryResponse = result.nextIfAvailable(1, TimeUnit.SECONDS);
        assertFalse(queryResponse.hasErrorMessage(),
                    () -> "Unexpected message: " + queryResponse.getErrorMessage().getMessage());
    }

    private void mockHandler(QueryRequest query, QueryHandler.ResponseHandler responseHandler) {
        responseHandler.sendLastResponse(QueryResponse.newBuilder().setPayload(query.getPayload()).build());
    }
}