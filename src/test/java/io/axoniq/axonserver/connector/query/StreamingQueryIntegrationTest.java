/*
 * Copyright (c) 2020-2022. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import io.axoniq.axonserver.connector.FlowControl;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.ProcessingKey;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class StreamingQueryIntegrationTest extends AbstractAxonServerIntegrationTest {

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
    void testStreamingQueryWhenPublisherCompletes() throws InterruptedException, TimeoutException {
        int maxResults = 100;
        connection1.queryChannel()
                   .registerQueryHandler(new SequencingQueryHandler(maxResults),
                                         new QueryDefinition("testQuery", "testResult"))
                   .awaitAck(1, TimeUnit.SECONDS);

        QueryRequest queryRequest = QueryRequest.newBuilder()
                                                .setQuery("testQuery")
                                                .addProcessingInstructions(clientSupportsStreaming())
                                                .build();
        ResultStream<QueryResponse> resultStream = connection2.queryChannel()
                                                              .query(queryRequest);

        List<String> results = new ArrayList<>();
        while (!resultStream.isClosed()) {
            QueryResponse result = resultStream.nextIfAvailable();
            if (result != null) {
                results.add(result.getPayload()
                                  .getData()
                                  .toStringUtf8());
            }
        }
        List<String> expected = IntStream.rangeClosed(1, maxResults)
                                         .map(i -> maxResults - i)
                                         .mapToObj(Objects::toString)
                                         .collect(Collectors.toList());
        assertEquals(expected, results);
    }

    @Test
    void testStreamingQueryWhenSubscriberCompletes() throws InterruptedException, TimeoutException {
        int maxResults = 100;
        connection1.queryChannel()
                   .registerQueryHandler(new SequencingQueryHandler(maxResults),
                                         new QueryDefinition("testQuery", "testResult"))
                   .awaitAck(1, TimeUnit.SECONDS);

        QueryRequest queryRequest = QueryRequest.newBuilder()
                                                .setQuery("testQuery")
                                                .addProcessingInstructions(clientSupportsStreaming())
                                                .build();
        ResultStream<QueryResponse> resultStream = connection2.queryChannel()
                                                              .query(queryRequest);

        List<String> results = new ArrayList<>();
        while (!resultStream.isClosed()) {
            QueryResponse result = resultStream.nextIfAvailable();
            if (result != null) {
                results.add(result.getPayload()
                                  .getData()
                                  .toStringUtf8());
                if (results.size() == 3) {
                    resultStream.close();
                }
            }
        }
        assertTrue(results.size() < maxResults);
    }

    @Test
    void testStreamingQueryWithMultiHandlers() throws InterruptedException, TimeoutException {
        QueryChannel publisherQueryChannel = connection1.queryChannel();
        int firstStreamMaxResults = 50;
        publisherQueryChannel.registerQueryHandler(new SequencingQueryHandler(firstStreamMaxResults, "publisher-1-"),
                                                   new QueryDefinition("testQuery", "testResult"))
                             .awaitAck(1, TimeUnit.SECONDS);
        int secondStreamMaxResults = 20;
        publisherQueryChannel.registerQueryHandler(new SequencingQueryHandler(secondStreamMaxResults, "publisher-2-"),
                                                   new QueryDefinition("testQuery", "testResult"))
                             .awaitAck(1, TimeUnit.SECONDS);

        QueryRequest queryRequest = QueryRequest.newBuilder()
                                                .setQuery("testQuery")
                                                .addProcessingInstructions(clientSupportsStreaming())
                                                .build();
        ResultStream<QueryResponse> resultStream = connection2.queryChannel()
                                                              .query(queryRequest);

        List<String> results = new ArrayList<>();
        while (!resultStream.isClosed()) {
            QueryResponse result = resultStream.nextIfAvailable();
            if (result != null) {
                results.add(result.getPayload()
                                  .getData()
                                  .toStringUtf8());
            }
        }

        List<String> expected = IntStream.rangeClosed(1, secondStreamMaxResults)
                                         .mapToObj(Objects::toString)
                                         .flatMap(i -> Stream.of(
                                                 "publisher-1-" + (firstStreamMaxResults - Integer.parseInt(i)),
                                                 "publisher-2-" + (secondStreamMaxResults - Integer.parseInt(i))))
                                         .collect(Collectors.toList());
        expected.addAll(IntStream.rangeClosed(secondStreamMaxResults + 1, firstStreamMaxResults)
                                 .mapToObj(i -> "publisher-1-" + (firstStreamMaxResults - i))
                                 .collect(Collectors.toList()));

        assertEquals(expected, results);
    }

    @Test
    void testStreamingQueryWithMultiHandlersWhenOneErrorsOut() throws InterruptedException, TimeoutException {
        QueryChannel publisherQueryChannel = connection1.queryChannel();
        int maxResults = 50;
        publisherQueryChannel.registerQueryHandler(new SequencingQueryHandler(maxResults),
                                                   new QueryDefinition("testQuery", "testResult"))
                             .awaitAck(1, TimeUnit.SECONDS);
        publisherQueryChannel.registerQueryHandler(new ErroringQueryHandler(),
                                                   new QueryDefinition("testQuery", "testResult"))
                             .awaitAck(1, TimeUnit.SECONDS);

        QueryRequest queryRequest = QueryRequest.newBuilder()
                                                .setQuery("testQuery")
                                                .addProcessingInstructions(clientSupportsStreaming())
                                                .build();
        ResultStream<QueryResponse> resultStream = connection2.queryChannel()
                                                              .query(queryRequest);

        List<String> results = new ArrayList<>();
        while (!resultStream.isClosed()) {
            QueryResponse result = resultStream.nextIfAvailable();
            if (result != null) {
                results.add(result.getPayload()
                                  .getData()
                                  .toStringUtf8());
            }
        }

        List<String> expected = IntStream.rangeClosed(1, maxResults)
                                         .map(i -> maxResults - i)
                                         .mapToObj(Objects::toString)
                                         .collect(Collectors.toList());

        assertEquals(expected, results);
    }

    @Test
    void testStreamingQueryWithMultiHandlersWhenAllErrorOut() throws InterruptedException, TimeoutException {
        QueryChannel publisherQueryChannel = connection1.queryChannel();
        publisherQueryChannel.registerQueryHandler(new ErroringQueryHandler(),
                                                   new QueryDefinition("testQuery", "testResult"))
                             .awaitAck(1, TimeUnit.SECONDS);
        publisherQueryChannel.registerQueryHandler(new ErroringQueryHandler(),
                                                   new QueryDefinition("testQuery", "testResult"))
                             .awaitAck(1, TimeUnit.SECONDS);

        QueryRequest queryRequest = QueryRequest.newBuilder()
                                                .setQuery("testQuery")
                                                .addProcessingInstructions(clientSupportsStreaming())
                                                .build();
        ResultStream<QueryResponse> resultStream = connection2.queryChannel()
                                                              .query(queryRequest);

        QueryResponse response = resultStream.next();

        assertTrue(response.hasErrorMessage());
    }

    @Test
    void testStreamingQueryResultsInError() throws InterruptedException, TimeoutException {
        connection1.queryChannel()
                   .registerQueryHandler(new ErroringQueryHandler(), new QueryDefinition("testQuery", "testResult"))
                   .awaitAck(1, TimeUnit.SECONDS);

        QueryRequest queryRequest = QueryRequest.newBuilder()
                                                .setQuery("testQuery")
                                                .addProcessingInstructions(clientSupportsStreaming())
                                                .build();
        ResultStream<QueryResponse> resultStream = connection2.queryChannel()
                                                              .query(queryRequest);

        QueryResponse response = resultStream.next();

        assertTrue(response.hasErrorMessage());
    }

    @Test
    void testStreamingQueryWhenFlowControlThrowsAnException() throws InterruptedException, TimeoutException {
        connection1.queryChannel()
                   .registerQueryHandler(new ExceptionThrowingQueryHandler(),
                                         new QueryDefinition("testQuery", "testResult"))
                   .awaitAck(1, TimeUnit.SECONDS);

        QueryRequest queryRequest = QueryRequest.newBuilder()
                                                .setQuery("testQuery")
                                                .addProcessingInstructions(clientSupportsStreaming())
                                                .build();
        ResultStream<QueryResponse> resultStream = connection2.queryChannel()
                                                              .query(queryRequest);

        QueryResponse response = resultStream.next();

        assertTrue(response.hasErrorMessage());
    }

    @NotNull
    private ProcessingInstruction clientSupportsStreaming() {
        return ProcessingInstruction.newBuilder()
                                    .setKey(ProcessingKey.CLIENT_SUPPORTS_STREAMING)
                                    .setValue(MetaDataValue.newBuilder()
                                                           .setBooleanValue(true)
                                                           .build())
                                    .build();
    }

    private static class ExceptionThrowingQueryHandler implements QueryHandler {

        @Override
        public void handle(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
            // noop
        }

        @Override
        public FlowControl stream(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
            return new FlowControl() {
                @Override
                public void request(long requested) {
                    throw new RuntimeException("oops");
                }

                @Override
                public void cancel() {
                    throw new RuntimeException("oops");
                }
            };
        }
    }

    private static class ErroringQueryHandler implements QueryHandler {

        @Override
        public void handle(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
            // noop
        }

        @Override
        public FlowControl stream(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
            return new FlowControl() {
                @Override
                public void request(long requested) {
                    QueryResponse response =
                            QueryResponse.newBuilder()
                                         .setErrorCode("errorCode")
                                         .setErrorMessage(ErrorMessage.getDefaultInstance())
                                         .build();
                    responseHandler.sendLast(response);
                }

                @Override
                public void cancel() {
                    responseHandler.complete();
                }
            };
        }
    }

    private static class SequencingQueryHandler implements QueryHandler {

        private final AtomicReference<ReplyChannel<QueryResponse>> replyChannelRef = new AtomicReference<>();
        private final String prefix;
        private final AtomicLong maxResults;

        public SequencingQueryHandler() {
            this(Long.MAX_VALUE);
        }

        public SequencingQueryHandler(long maxResults) {
            this(maxResults, "");
        }

        public SequencingQueryHandler(long maxResults, String prefix) {
            this.prefix = prefix;
            this.maxResults = new AtomicLong(maxResults);
        }

        @Override
        public void handle(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
            // noop
        }

        @Override
        public FlowControl stream(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
            replyChannelRef.set(responseHandler);
            return new FlowControl() {
                @Override
                public void request(long requested) {
                    for (long i = 0; i < requested; i++) {
                        if (maxResults.getAndDecrement() <= 0) {
                            complete();
                            break;
                        }
                        ByteString data = ByteString.copyFromUtf8(prefix + maxResults.get());
                        SerializedObject payload = SerializedObject.newBuilder()
                                                                   .setType("java.lang.String")
                                                                   .setData(data)
                                                                   .build();
                        QueryResponse message = QueryResponse.newBuilder()
                                                             .setPayload(payload)
                                                             .setRequestIdentifier(query.getMessageIdentifier())
                                                             .build();
                        responseHandler.send(message);
                    }
                }

                @Override
                public void cancel() {
                    responseHandler.complete();
                }
            };
        }

        public void complete() {
            ReplyChannel<QueryResponse> replyChannel = replyChannelRef.get();
            if (replyChannel != null) {
                replyChannel.complete();
            }
        }
    }
}
