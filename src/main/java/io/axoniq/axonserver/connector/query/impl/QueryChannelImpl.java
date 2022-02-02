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

package io.axoniq.axonserver.connector.query.impl;

import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.InstructionHandler;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.connector.impl.AbstractIncomingInstructionStream;
import io.axoniq.axonserver.connector.impl.AsyncRegistration;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.BufferingReplyChannel;
import io.axoniq.axonserver.connector.impl.CloseAwareReplyChannel;
import io.axoniq.axonserver.connector.impl.DisposableReadonlyBuffer;
import io.axoniq.axonserver.connector.impl.FlowControlledReplyChannelWriter;
import io.axoniq.axonserver.connector.impl.NoopFlowControl;
import io.axoniq.axonserver.connector.impl.ObjectUtils;
import io.axoniq.axonserver.connector.impl.buffer.BlockingCloseableBuffer;
import io.axoniq.axonserver.connector.impl.buffer.FlowControlledDisposableReadonlyBuffer;
import io.axoniq.axonserver.connector.query.QueryChannel;
import io.axoniq.axonserver.connector.query.QueryDefinition;
import io.axoniq.axonserver.connector.query.QueryHandler;
import io.axoniq.axonserver.connector.query.SubscriptionQueryResult;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.ProcessingKey;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.query.QueryComplete;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryProviderOutbound;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryServiceGrpc;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.QueryUpdateComplete;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;

/**
 * {@link QueryChannel} implementation, serving as the query connection between AxonServer and a client application.
 */
public class QueryChannelImpl extends AbstractAxonServerChannel<QueryProviderOutbound> implements QueryChannel {

    private static final Logger logger = LoggerFactory.getLogger(QueryChannelImpl.class);

    private static final QueryResponse TERMINAL = QueryResponse.newBuilder().setErrorCode("__TERMINAL__").build();

    private final AtomicReference<CallStreamObserver<QueryProviderOutbound>> outboundQueryStream = new AtomicReference<>();
    private final Map<QueryDefinition, AtomicInteger> supportedQueries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<QueryHandler>> queryHandlers = new ConcurrentHashMap<>();
    private final ConcurrentMap<Enum<?>, InstructionHandler<QueryProviderInbound, QueryProviderOutbound>> instructionHandlers = new ConcurrentHashMap<>();

    private final ClientIdentification clientIdentification;
    private final String context;
    private final int permits;
    private final int permitsBatch;

    // monitor for modification access to queryHandlers collection
    private final Object queryHandlerMonitor = new Object();
    private final Map<String, Set<Registration>> subscriptionQueries = new ConcurrentHashMap<>();
    private final QueryServiceGrpc.QueryServiceStub queryServiceStub;
    private final Map<String, QueryInProgress> queriesInProgress = new ConcurrentHashMap<>();
    private final AtomicBoolean subscriptionsCompleted = new AtomicBoolean(false);

    /**
     * Constructs a {@link QueryChannelImpl}.
     *
     * @param clientIdentification client information identifying whom has connected. This information is used to pass
     *                             on to message
     * @param permits              an {@code int} defining the number of permits this channel has
     * @param permitsBatch         an {@code int} defining the number of permits to be consumed from prior to requesting
     *                             additional permits for this channel
     * @param executor             a {@link ScheduledExecutorService} used to schedule reconnects of this channel
     * @param channel              the {@link AxonServerManagedChannel} used to form the connection with AxonServer
     */
    public QueryChannelImpl(ClientIdentification clientIdentification,
                            String context,
                            int permits,
                            int permitsBatch,
                            ScheduledExecutorService executor,
                            AxonServerManagedChannel channel) {
        super(clientIdentification, executor, channel);
        this.clientIdentification = clientIdentification;
        this.context = context;
        this.permits = permits;
        this.permitsBatch = permitsBatch;
        instructionHandlers.put(QueryProviderInbound.RequestCase.QUERY, this::handleQuery);
        instructionHandlers.put(QueryProviderInbound.RequestCase.ACK, this::handleAck);
        instructionHandlers.put(SubscriptionQueryRequest.RequestCase.GET_INITIAL_RESULT, this::getInitialResult);
        instructionHandlers.put(SubscriptionQueryRequest.RequestCase.SUBSCRIBE, this::subscribeToQueryUpdates);
        instructionHandlers.put(SubscriptionQueryRequest.RequestCase.UNSUBSCRIBE, this::unsubscribeToQueryUpdates);
        instructionHandlers.put(QueryProviderInbound.RequestCase.QUERY_COMPLETE, this::handleCancelRequest);
        instructionHandlers.put(QueryProviderInbound.RequestCase.QUERY_FLOW_CONTROL, this::handleFlowControlRequest);
        queryServiceStub = QueryServiceGrpc.newStub(channel);
    }

    private void handleAck(QueryProviderInbound query, ReplyChannel<QueryProviderOutbound> result) {
        processAck(query.getAck());
        result.complete();
    }

    private void unsubscribeToQueryUpdates(QueryProviderInbound query, ReplyChannel<QueryProviderOutbound> result) {
        SubscriptionQuery unsubscribe = query.getSubscriptionQueryRequest().getUnsubscribe();
        Set<Registration> registration = subscriptionQueries.remove(unsubscribe.getSubscriptionIdentifier());
        if (registration != null) {
            registration.forEach(Registration::cancel);
        }
        result.complete();
    }

    private void subscribeToQueryUpdates(QueryProviderInbound query, ReplyChannel<QueryProviderOutbound> result) {
        final SubscriptionQuery subscribe = query.getSubscriptionQueryRequest().getSubscribe();
        final String subscriptionIdentifier = subscribe.getSubscriptionIdentifier();
        Set<QueryHandler> handlers =
                queryHandlers.getOrDefault(subscribe.getQueryRequest().getQuery(), Collections.emptySet());
        handlers.forEach(e -> {
            Registration registration = e.registerSubscriptionQuery(subscribe, new QueryHandler.UpdateHandler() {
                @Override
                public void sendUpdate(QueryUpdate queryUpdate) {
                    SubscriptionQueryResponse subscriptionQueryUpdate =
                            SubscriptionQueryResponse.newBuilder()
                                                     .setSubscriptionIdentifier(subscriptionIdentifier)
                                                     .setUpdate(queryUpdate)
                                                     .setMessageIdentifier(queryUpdate.getMessageIdentifier())
                                                     .build();
                    result.send(QueryProviderOutbound.newBuilder()
                                                     .setSubscriptionQueryResponse(subscriptionQueryUpdate)
                                                     .build());
                    logger.debug("Subscription Query Update [id: {}] for subscription {}, sent to client {}.",
                                 queryUpdate.getMessageIdentifier(),
                                 subscribe.getSubscriptionIdentifier(),
                                 queryUpdate.getClientId());
                }

                @Override
                public void complete() {
                    QueryUpdateComplete complete =
                            QueryUpdateComplete.newBuilder()
                                               .setClientId(clientIdentification.getClientId())
                                               .setComponentName(clientIdentification.getComponentName())
                                               .build();
                    SubscriptionQueryResponse subscriptionQueryResult =
                            SubscriptionQueryResponse.newBuilder()
                                                     .setSubscriptionIdentifier(subscriptionIdentifier)
                                                     .setComplete(complete)
                                                     .build();
                    result.send(QueryProviderOutbound.newBuilder()
                                                     .setSubscriptionQueryResponse(subscriptionQueryResult)
                                                     .build());
                    logger.debug("Subscription Query Update completion sent to client {}.",
                                 complete.getClientId());
                }
            });
            if (registration != null) {
                subscriptionQueries.compute(
                        subscriptionIdentifier, (k, v) -> v != null ? v : new CopyOnWriteArraySet<>()
                ).add(registration);
            }
        });
        result.complete();
    }

    private void handleCancelRequest(QueryProviderInbound complete, ReplyChannel<QueryProviderOutbound> result) {
        queriesInProgress.getOrDefault(complete.getQueryComplete().getRequestId(), QueryInProgress.noop())
                         .cancel();
        result.complete();
    }

    private void handleFlowControlRequest(QueryProviderInbound flowControl,
                                          ReplyChannel<QueryProviderOutbound> result) {
        queriesInProgress.getOrDefault(flowControl.getQueryFlowControl().getRequestId(), QueryInProgress.noop())
                         .request(flowControl.getQueryFlowControl().getPermits());
        result.complete();
    }

    @Override
    public void connect() {
        if (!queryHandlers.isEmpty()) {
            doConnectQueryStream();
        }
    }

    private synchronized void doConnectQueryStream() {
        if (outboundQueryStream.get() != null) {
            logger.debug("QueryChannel for context '{}' is already connected", context);
            return;
        }

        this.subscriptionsCompleted.set(queryHandlers.isEmpty());

        IncomingQueryInstructionStream responseObserver = new IncomingQueryInstructionStream(
                clientIdentification.getClientId(),
                permits,
                permitsBatch,
                this::onConnectionError,
                this::registerOutboundStream
        );

        //noinspection ResultOfMethodCallIgnored
        queryServiceStub.openStream(responseObserver);
        CallStreamObserver<QueryProviderOutbound> newValue = responseObserver.getInstructionsForPlatform();

        supportedQueries.keySet().stream()
                        .map(queryDef -> sendInstruction(buildSubscribeMessage(queryDef.getQueryName(), queryDef.getResultType(), UUID.randomUUID().toString()),
                                                         QueryProviderOutbound::getInstructionId,
                                                         newValue))
                        .reduce(CompletableFuture::allOf)
                        .map(cf -> cf.exceptionally(e -> {
                            logger.warn("An error occurred while registering query handlers", e);
                            return null;
                        }))
                        .orElse(CompletableFuture.completedFuture(null))
                        .thenRun(() -> subscriptionsCompleted.set(true));

        logger.info("QueryChannel for context '{}' connected, {} registrations resubscribed", context, queryHandlers.size());
        responseObserver.enableFlowControl();
    }

    private void onConnectionError(Throwable error) {
        logger.info("Error on QueryChannel for context {}", context, error);
        scheduleReconnect(error);
    }

    private void registerOutboundStream(CallStreamObserver<QueryProviderOutbound> upstream) {
        StreamObserver<QueryProviderOutbound> previous = outboundQueryStream.getAndSet(upstream);
        ObjectUtils.silently(previous, StreamObserver::onCompleted);
    }

    private QueryProviderOutbound buildSubscribeMessage(String queryName, String resultName, String instructionId) {
        QuerySubscription.Builder querySubscription =
                QuerySubscription.newBuilder()
                                 .setMessageId(instructionId)
                                 .setQuery(queryName)
                                 .setResultName(resultName)
                                 .setClientId(clientIdentification.getClientId())
                                 .setComponentName(clientIdentification.getComponentName());
        return QueryProviderOutbound.newBuilder()
                                    .setInstructionId(instructionId)
                                    .setSubscribe(querySubscription)
                                    .build();
    }

    @Override
    public Registration registerQueryHandler(QueryHandler handler, QueryDefinition... queryDefinitions) {
        CompletableFuture<Void> subscriptionResult = CompletableFuture.completedFuture(null);
        synchronized (queryHandlerMonitor) {
            if (queryHandlers.isEmpty()) {
                doConnectQueryStream();
            }

            for (QueryDefinition queryDefinition : queryDefinitions) {
                this.queryHandlers.computeIfAbsent(queryDefinition.getQueryName(), k -> new CopyOnWriteArraySet<>())
                                  .add(handler);
                boolean firstRegistration = supportedQueries.computeIfAbsent(queryDefinition, k -> new AtomicInteger())
                                                            .getAndIncrement() == 0;
                if (firstRegistration) {
                    QueryProviderOutbound subscribeMessage = buildSubscribeMessage(queryDefinition.getQueryName(),
                                                                                   queryDefinition.getResultType(),
                                                                                   UUID.randomUUID().toString());
                    CompletableFuture<Void> instructionResult = sendInstruction(subscribeMessage,
                                                                                QueryProviderOutbound::getInstructionId,
                                                                                outboundQueryStream.get());
                    subscriptionResult = CompletableFuture.allOf(subscriptionResult, instructionResult);
                }
                logger.info("Registered handler for query '{}' in context '{}'", queryDefinition, context);
            }
        }
        return new AsyncRegistration(subscriptionResult, () -> {
            synchronized (queryHandlerMonitor) {
                CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
                for (QueryDefinition def : queryDefinitions) {
                    Set<QueryHandler> refs = queryHandlers.get(def.getQueryName());
                    if (refs != null && refs.remove(handler) && refs.isEmpty()) {
                        queryHandlers.remove(def.getQueryName());
                        result = CompletableFuture.allOf(result, sendUnsubscribe(def, outboundQueryStream.get()));
                        logger.debug("Unregistered handlers for query '{}' in context '{}'", def, context);
                    }
                    supportedQueries.computeIfPresent(def, (qd, counter) -> counter.decrementAndGet() == 0 ? null : counter);
                }
                return result;
            }
        });
    }

    private CompletableFuture<Void> sendUnsubscribe(QueryDefinition queryDefinition, StreamObserver<QueryProviderOutbound> outboundStream) {
        if (outboundStream == null) {
            return CompletableFuture.completedFuture(null);
        }
        String instructionId = UUID.randomUUID().toString();
        QuerySubscription unsubscribeMessage =
                QuerySubscription.newBuilder()
                                 .setMessageId(instructionId)
                                 .setQuery(queryDefinition.getQueryName())
                                 .setResultName(queryDefinition.getResultType())
                                 .setClientId(clientIdentification.getClientId())
                                 .setComponentName(clientIdentification.getComponentName())
                                 .build();
        return sendInstruction(QueryProviderOutbound.newBuilder()
                                                    .setInstructionId(instructionId)
                                                    .setUnsubscribe(unsubscribeMessage)
                                                    .build(),
                               QueryProviderOutbound::getInstructionId,
                               outboundStream);
    }

    @Override
    public ResultStream<QueryResponse> query(QueryRequest query) {
        if (query.getMessageIdentifier().isEmpty()) {
            query = query.toBuilder().setMessageIdentifier(UUID.randomUUID().toString()).build();
        }
        AbstractBufferedStream<QueryResponse, QueryRequest> results = new AbstractBufferedStream<QueryResponse, QueryRequest>(
                clientIdentification.getClientId(), 32, 8
        ) {

            @Override
            protected QueryRequest buildFlowControlMessage(FlowControl flowControl) {
                return null;
            }

            @Override
            protected QueryResponse terminalMessage() {
                return TERMINAL;
            }

            @Override
            public void close() {
                outboundStream().cancel("Client cancelled the stream.", null);
            }
        };
        if ("".equals(query.getMessageIdentifier())) {
            logger.debug("No message identifier has been set on the query. Adding a random identifier now.");
            query = query.toBuilder().setMessageIdentifier(UUID.randomUUID().toString()).build();
        }
        queryServiceStub.query(query, results);
        return results;
    }

    @Override
    public SubscriptionQueryResult subscriptionQuery(QueryRequest query,
                                                     SerializedObject updateResponseType,
                                                     int bufferSize,
                                                     int fetchSize) {
        QueryRequest finalQuery;
        if (query.getMessageIdentifier().isEmpty()) {
            finalQuery = query.toBuilder().setMessageIdentifier(UUID.randomUUID().toString()).build();
        } else {
            finalQuery = query;
        }
        String subscriptionId = finalQuery.getMessageIdentifier();
        CompletableFuture<QueryResponse> initialResultFuture = new CompletableFuture<>();
        SubscriptionQueryStream subscriptionStream = new SubscriptionQueryStream(
                subscriptionId, initialResultFuture, QueryChannelImpl.this.clientIdentification.getClientId(),
                bufferSize, fetchSize
        );
        StreamObserver<SubscriptionQueryRequest> upstream = queryServiceStub.subscription(subscriptionStream);
        subscriptionStream.enableFlowControl();
        SubscriptionQuery subscriptionQuery = SubscriptionQuery.newBuilder()
                                                               .setQueryRequest(finalQuery)
                                                               .setSubscriptionIdentifier(subscriptionId)
                                                               .setUpdateResponseType(updateResponseType)
                                                               .build();
        upstream.onNext(SubscriptionQueryRequest.newBuilder().setSubscribe(subscriptionQuery).build());
        return new SubscriptionQueryResult() {

            private final AtomicBoolean initialResultRequested = new AtomicBoolean();

            @Override
            public CompletableFuture<QueryResponse> initialResult() {
                if (!initialResultFuture.isDone() && !initialResultRequested.getAndSet(true)) {
                    SubscriptionQuery.Builder initialResultRequest =
                            SubscriptionQuery.newBuilder()
                                             .setQueryRequest(finalQuery)
                                             .setSubscriptionIdentifier(subscriptionId);
                    upstream.onNext(SubscriptionQueryRequest.newBuilder()
                                                            .setGetInitialResult(initialResultRequest)
                                                            .build());
                }
                return initialResultFuture;
            }

            @Override
            public ResultStream<QueryUpdate> updates() {
                return subscriptionStream.buffer();
            }
        };
    }

    @Override
    public synchronized void disconnect() {
        CallStreamObserver<QueryProviderOutbound> previousOutbound = outboundQueryStream.getAndSet(null);

        CompletableFuture<Void> unsubscribed = previousOutbound == null
                                               ? CompletableFuture.completedFuture(null)
                                               : supportedQueries.keySet()
                                                                 .stream()
                                                                 .map(queryDefinition -> sendUnsubscribe(queryDefinition, previousOutbound))
                                                                 .reduce(CompletableFuture::allOf)
                                                                 .map(cf -> cf.exceptionally(e -> {
                                                                     logger.warn("An error occurred while unregistering query handlers", e);
                                                                     return null;
                                                                 }))
                                                                 .orElseGet(() -> CompletableFuture.completedFuture(null));
        cancelAllSubscriptionQueries();

        unsubscribed.thenCompose(stream -> {
            if (!queriesInProgress.isEmpty()) {
                logger.info("Disconnect requested. Waiting for {} queries to be completed", queriesInProgress.size());
            }
            return queriesInProgress.values()
                                    .stream()
                                    .map(QueryInProgress::whenComplete)
                                    .reduce(CompletableFuture::allOf)
                                    .orElseGet(() -> CompletableFuture.completedFuture(null));
        }).thenAccept(previousStream -> doIfNotNull(previousOutbound, StreamObserver::onCompleted));
    }

    @Override
    public void reconnect() {
        disconnect();
        scheduleImmediateReconnect();
    }

    @Override
    public CompletableFuture<Void> prepareDisconnect() {
        CallStreamObserver<QueryProviderOutbound> outboundStream = outboundQueryStream.get();
        CompletableFuture<Void> future = supportedQueries.keySet()
                                                         .stream()
                                                         .map(queryDefinition -> sendUnsubscribe(queryDefinition, outboundStream))
                                                         .reduce(CompletableFuture::allOf)
                                                         .orElseGet(() -> CompletableFuture.completedFuture(null));
        cancelAllSubscriptionQueries();
        return future;
    }

    private void cancelAllSubscriptionQueries() {
        subscriptionQueries.forEach((k, v) -> subscriptionQueries.remove(k).forEach(Registration::cancel));
    }

    @Override
    public boolean isReady() {
        return queryHandlers.isEmpty() || (outboundQueryStream.get() != null && subscriptionsCompleted.get());
    }

    private void doHandleQuery(QueryProviderInbound query, ReplyChannel<QueryResponse> responseHandler) {
        doHandleQuery(query.getQuery(), responseHandler);
    }

    private void doHandleQuery(QueryRequest query, ReplyChannel<QueryResponse> responseChannel) {
        AtomicReference<io.axoniq.axonserver.connector.FlowControl> flowControlRef = new AtomicReference<>();
        Runnable removeQuery = () -> queriesInProgress.remove(query.getMessageIdentifier());
        QueryInProgress queryInProgress = new QueryInProgress(removeQuery, flowControlRef::get);
        if (queriesInProgress.putIfAbsent(query.getMessageIdentifier(), queryInProgress) != null) {
            return;
        }
        ReplyChannel<QueryResponse> responseHandler = new CloseAwareReplyChannel<>(responseChannel,
                                                                                   queryInProgress::cancel);
        Set<QueryHandler> handlers = queryHandlers.getOrDefault(query.getQuery(), Collections.emptySet());
        if (handlers.isEmpty()) {
            responseHandler.sendNack();
            responseHandler.sendLast(QueryResponse.newBuilder()
                                                  .setRequestIdentifier(query.getMessageIdentifier())
                                                  .setErrorCode(ErrorCategory.NO_HANDLER_FOR_QUERY.errorCode())
                                                  .setErrorMessage(ErrorMessage.newBuilder()
                                                                               .setMessage("No handler for query")
                                                                               .build())
                                                  .build());
        }

        responseHandler.sendAck();

        List<DisposableReadonlyBuffer<QueryResponse>> buffers =
                handlers.stream()
                        .map(queryHandler -> executeQuery(queryHandler, query, responseChannel))
                        .collect(Collectors.toList());

        ReplyChannel<QueryResponse> requestIdReplenishmentReplyChannel =
                new RequestIdEnhancementReplyChannel(query, responseHandler);
        FlowControlledReplyChannelWriter<QueryResponse> flowControl =
                new FlowControlledReplyChannelWriter<>(buffers, requestIdReplenishmentReplyChannel);
        flowControlRef.set(flowControl);
        if (!supportsStreaming(query)) {
            // if streaming is not supported by axon server(s) or query sender we have to be eager in requesting
            flowControl.request(Long.MAX_VALUE);
        }
    }

    private DisposableReadonlyBuffer<QueryResponse> executeQuery(QueryHandler handler,
                                                                 QueryRequest query,
                                                                 ReplyChannel<QueryResponse> replyChannel) {
        BlockingCloseableBuffer<QueryResponse> buffer = new BlockingCloseableBuffer<>();
        BufferingReplyChannel<QueryResponse> bufferingReplyChannel = new BufferingReplyChannel<>(replyChannel, buffer);
        return new FlowControlledDisposableReadonlyBuffer<>(handler.stream(query, bufferingReplyChannel), buffer);
    }

    private boolean supportsStreaming(QueryRequest queryRequest) {
        return axonServerSupportsQueryStreaming(queryRequest) && querySenderSupportsStreaming(queryRequest);
    }

    private boolean axonServerSupportsQueryStreaming(QueryRequest queryRequest) {
        return queryRequest.getProcessingInstructionsList()
                           .stream()
                           .filter(instruction -> ProcessingKey.SUPPORTS_STREAMING.equals(instruction.getKey()))
                           .map(instruction -> instruction.getValue().getBooleanValue())
                           .findFirst()
                           .orElse(false);
    }

    private boolean querySenderSupportsStreaming(QueryRequest queryRequest) {
        return !"".equals(queryRequest.getExpectedResponseType());
    }

    private void handleQuery(QueryProviderInbound inbound, ReplyChannel<QueryProviderOutbound> result) {
        doHandleQuery(inbound, new ReplyChannel<QueryResponse>() {
            @Override
            public void send(QueryResponse response) {
                result.send(QueryProviderOutbound.newBuilder().setQueryResponse(response).build());
            }

            @Override
            public void complete() {
                QueryComplete queryComplete = QueryComplete.newBuilder()
                                                           .setRequestId(inbound.getQuery().getMessageIdentifier())
                                                           .setMessageId(UUID.randomUUID().toString())
                                                           .build();
                result.send(QueryProviderOutbound.newBuilder()
                                                 .setQueryComplete(queryComplete)
                                                 .build());
                result.complete();
            }

            @Override
            public void completeWithError(ErrorMessage errorMessage) {
                result.completeWithError(errorMessage);
            }

            @Override
            public void completeWithError(ErrorCategory errorCategory, String message) {
                result.completeWithError(errorCategory, message);
            }

            @Override
            public void sendNack(ErrorMessage errorMessage) {
                result.sendNack(errorMessage);
            }

            @Override
            public void sendAck() {
                result.sendAck();
            }
        });
    }

    private void getInitialResult(QueryProviderInbound query, ReplyChannel<QueryProviderOutbound> result) {
        String subscriptionId = query.getSubscriptionQueryRequest().getGetInitialResult().getSubscriptionIdentifier();
        doHandleQuery(
                query.getSubscriptionQueryRequest().getGetInitialResult().getQueryRequest(),
                new ReplyChannel<QueryResponse>() {

                    @Override
                    public void send(QueryResponse response) {
                        SubscriptionQueryResponse initialResult =
                                SubscriptionQueryResponse.newBuilder()
                                                         .setSubscriptionIdentifier(subscriptionId)
                                                         .setInitialResult(response)
                                                         .setMessageIdentifier(response.getMessageIdentifier())
                                                         .build();
                        result.send(QueryProviderOutbound.newBuilder()
                                                         .setSubscriptionQueryResponse(initialResult)
                                                         .build());
                    }

                    @Override
                    public void complete() {
                        result.complete();
                    }

                    @Override
                    public void completeWithError(ErrorMessage errorMessage) {
                        result.completeWithError(errorMessage);
                    }

                    @Override
                    public void completeWithError(ErrorCategory errorCategory, String message) {
                        result.completeWithError(errorCategory, message);
                    }

                    @Override
                    public void sendNack(ErrorMessage errorMessage) {
                        result.sendNack(errorMessage);
                    }

                    @Override
                    public void sendAck() {
                        result.sendAck();
                    }
                }
        );
    }

    private static class QueryInProgress {

        private final CompletableFuture<?> cancelHandler;
        private final Supplier<io.axoniq.axonserver.connector.FlowControl> flowControlSupplier;

        public static QueryInProgress noop() {
            return new QueryInProgress(() -> {
            }, () -> NoopFlowControl.INSTANCE);
        }

        public QueryInProgress(Runnable cancelHandler,
                               Supplier<io.axoniq.axonserver.connector.FlowControl> flowControlSupplier) {
            this.cancelHandler = new CompletableFuture<>();
            this.flowControlSupplier = flowControlSupplier;
            this.cancelHandler.whenComplete((r, e) -> {
                io.axoniq.axonserver.connector.FlowControl flowControl = flowControlSupplier.get();
                if (flowControl != null) {
                    flowControl.cancel();
                }
                cancelHandler.run();
            });
        }

        public CompletableFuture<?> whenComplete() {
            return cancelHandler;
        }

        public void cancel() {
            whenComplete().complete(null);
        }

        public void request(long requested) {
            io.axoniq.axonserver.connector.FlowControl flowControl = flowControlSupplier.get();
            if (flowControl != null) {
                flowControl.request(requested);
            }
        }
    }

    private static class RequestIdEnhancementReplyChannel implements ReplyChannel<QueryResponse> {

        private final QueryRequest query;
        private final ReplyChannel<QueryResponse> delegate;

        public RequestIdEnhancementReplyChannel(QueryRequest query, ReplyChannel<QueryResponse> delegate) {
            this.query = query;
            this.delegate = delegate;
        }

        @Override
        public void send(QueryResponse response) {
            if (!query.getMessageIdentifier()
                      .equals(response.getRequestIdentifier())) {
                logger.debug("RequestIdentifier not properly set, modifying message");
                QueryResponse newResponse = response.toBuilder()
                                                    .setRequestIdentifier(query.getMessageIdentifier())
                                                    .build();
                delegate.send(newResponse);
            } else {
                delegate.send(response);
            }
        }

        @Override
        public void complete() {
            delegate.complete();
        }

        @Override
        public void completeWithError(ErrorMessage errorMessage) {
            delegate.completeWithError(errorMessage);
        }

        @Override
        public void completeWithError(ErrorCategory errorCategory, String message) {
            delegate.completeWithError(errorCategory, message);
        }

        @Override
        public void sendNack(ErrorMessage errorMessage) {
            delegate.sendNack(errorMessage);
        }

        @Override
        public void sendAck() {
            delegate.sendAck();
        }
    }

    private class IncomingQueryInstructionStream
            extends AbstractIncomingInstructionStream<QueryProviderInbound, QueryProviderOutbound> {

        public IncomingQueryInstructionStream(String clientId,
                                              int permits,
                                              int permitsBatch,
                                              Consumer<Throwable> disconnectHandler,
                                              Consumer<CallStreamObserver<QueryProviderOutbound>> beforeStartHandler) {
            super(clientId, permits, permitsBatch, disconnectHandler, beforeStartHandler);
        }

        @Override
        protected QueryProviderOutbound buildFlowControlMessage(FlowControl flowControl) {
            return QueryProviderOutbound.newBuilder().setFlowControl(flowControl).build();
        }

        @Override
        protected QueryProviderOutbound buildAckMessage(InstructionAck ack) {
            return QueryProviderOutbound.newBuilder().setAck(ack).build();
        }

        @Override
        protected String getInstructionId(QueryProviderInbound instruction) {
            return instruction.getInstructionId();
        }

        @Override
        protected InstructionHandler<QueryProviderInbound, QueryProviderOutbound> getHandler(
                QueryProviderInbound request
        ) {
            if (request.getRequestCase() == QueryProviderInbound.RequestCase.SUBSCRIPTION_QUERY_REQUEST) {
                return instructionHandlers.get(request.getSubscriptionQueryRequest().getRequestCase());
            }
            return instructionHandlers.get(request.getRequestCase());
        }

        @Override
        protected boolean unregisterOutboundStream(CallStreamObserver<QueryProviderOutbound> expected) {
            if (outboundQueryStream.compareAndSet(expected, null)) {
                cancelAllSubscriptionQueries();
                return true;
            }
            return false;
        }
    }
}
