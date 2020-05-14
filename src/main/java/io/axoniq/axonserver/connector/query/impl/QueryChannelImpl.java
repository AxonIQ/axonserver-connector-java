package io.axoniq.axonserver.connector.query.impl;

import io.axoniq.axonserver.connector.ErrorCode;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.connector.impl.AbstractIncomingInstructionStream;
import io.axoniq.axonserver.connector.impl.ObjectUtils;
import io.axoniq.axonserver.connector.query.QueryChannel;
import io.axoniq.axonserver.connector.query.QueryDefinition;
import io.axoniq.axonserver.connector.query.QueryHandler;
import io.axoniq.axonserver.connector.query.SubscriptionQueryResult;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.InstructionAck;
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
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;

public class QueryChannelImpl extends AbstractAxonServerChannel implements QueryChannel {

    private static final Logger logger = LoggerFactory.getLogger(QueryChannel.class);
    private final AtomicReference<QueryServiceGrpc.QueryServiceStub> queryService = new AtomicReference<>();
    private final AtomicReference<StreamObserver<QueryProviderOutbound>> outboundQueryStream = new AtomicReference<>();
    private final Set<QueryDefinition> supportedQueries = new CopyOnWriteArraySet<>();
    private final ConcurrentMap<String, Set<QueryHandler>> queryHandlers = new ConcurrentHashMap<>();
    private final ConcurrentMap<Enum<?>, BiConsumer<QueryProviderInbound, ReplyChannel<QueryProviderOutbound>>> instructionHandlers = new ConcurrentHashMap<>();

    // TODO - keep track of subscriptions to close them when closing this channel
    private final ClientIdentification clientIdentification;
    private final int permits;
    private final int permitsBatch;

    // monitor for modification access to queryHandlers collection
    private final Object queryHandlerMonitor = new Object();
    private final Map<String, Set<Registration>> subscriptionQueries = new ConcurrentHashMap<>();

    public QueryChannelImpl(ClientIdentification clientIdentification,
                            int permits, int permitsBatch,
                            ScheduledExecutorService executor,
                            ManagedChannel channel) {
        super(executor, channel);
        this.clientIdentification = clientIdentification;
        this.permits = permits;
        this.permitsBatch = permitsBatch;
        instructionHandlers.put(QueryProviderInbound.RequestCase.QUERY, this::handleQuery);
        instructionHandlers.put(QueryProviderInbound.RequestCase.ACK, this::handleAck);
        instructionHandlers.put(SubscriptionQueryRequest.RequestCase.GET_INITIAL_RESULT, this::getInitialResult);
        instructionHandlers.put(SubscriptionQueryRequest.RequestCase.SUBSCRIBE, this::subscribeToQueryUpdates);
        instructionHandlers.put(SubscriptionQueryRequest.RequestCase.UNSUBSCRIBE, this::unsubscribeToQueryUpdates);
    }

    private void handleAck(QueryProviderInbound query, ReplyChannel<QueryProviderOutbound> result) {
        // TODO - Keep track of instructions awaiting ACK and complete them
        result.markConsumed();
    }

    private void unsubscribeToQueryUpdates(QueryProviderInbound query, ReplyChannel<QueryProviderOutbound> result) {
        SubscriptionQuery unsubscribe = query.getSubscriptionQueryRequest().getUnsubscribe();
        Set<Registration> registration = subscriptionQueries.remove(unsubscribe.getSubscriptionIdentifier());
        if (registration != null) {
            registration.forEach(Registration::cancel);
        }
    }

    private void subscribeToQueryUpdates(QueryProviderInbound query, ReplyChannel<QueryProviderOutbound> result) {
        final SubscriptionQuery subscribe = query.getSubscriptionQueryRequest().getSubscribe();
        final String subscriptionIdentifier = subscribe.getSubscriptionIdentifier();
        Set<QueryHandler> handlers = queryHandlers.getOrDefault(subscribe.getQueryRequest().getQuery(), Collections.emptySet());
        handlers.forEach(e -> {
            Registration registration = e.registerSubscriptionQuery(subscribe.getQueryRequest(), new QueryHandler.UpdateHandler() {
                @Override
                public void sendUpdate(QueryUpdate response) {
                    result.send(QueryProviderOutbound.newBuilder().setSubscriptionQueryResponse(SubscriptionQueryResponse.newBuilder().setSubscriptionIdentifier(subscriptionIdentifier).setUpdate(response).build()).build());
                }

                @Override
                public void complete() {
                    result.send(QueryProviderOutbound.newBuilder().setSubscriptionQueryResponse(SubscriptionQueryResponse.newBuilder().setSubscriptionIdentifier(subscriptionIdentifier).setComplete(QueryUpdateComplete.newBuilder().setClientId(clientIdentification.getClientId()).setComponentName(clientIdentification.getComponentName()).build()).build()).build());
                }
            });
            if (registration != null) {
                subscriptionQueries.compute(subscriptionIdentifier, (k, v) -> v != null ? v : new CopyOnWriteArraySet<>()).add(registration);
            }
        });
    }

    @Override
    public void connect(ManagedChannel channel) {
        if (outboundQueryStream.get() != null) {
            // we're already connected on this channel
            return;
        }
        QueryServiceGrpc.QueryServiceStub queryServiceStub = QueryServiceGrpc.newStub(channel);
        IncomingQueryInstructionStream responseObserver = new IncomingQueryInstructionStream(clientIdentification.getClientId(),
                                                                                             permits, permitsBatch,
                                                                                             e -> scheduleReconnect());
        StreamObserver<QueryProviderOutbound> newValue = queryServiceStub.openStream(responseObserver);

        StreamObserver<QueryProviderOutbound> previous = outboundQueryStream.getAndSet(newValue);

        supportedQueries.forEach(k -> newValue.onNext(buildSubscribeMessage(k.getQueryName(), k.getResultType(), UUID.randomUUID().toString())));
        responseObserver.enableFlowControl();


        logger.info("QueryChannel connected, {} query types registered", queryHandlers.size());
        ObjectUtils.silently(previous, StreamObserver::onCompleted);

        queryService.getAndSet(queryServiceStub);

    }

    private QueryProviderOutbound buildSubscribeMessage(String queryName, String resultName, String instructionId) {
        return QueryProviderOutbound.newBuilder()
                                    .setInstructionId(instructionId)
                                    .setSubscribe(QuerySubscription.newBuilder()
                                                                   .setMessageId(instructionId)
                                                                   .setQuery(queryName)
                                                                   .setResultName(resultName)
                                                                   .setClientId(clientIdentification.getClientId())
                                                                   .setComponentName(clientIdentification.getComponentName()))
                                    .build();
    }

    @Override
    public Registration registerQueryHandler(QueryHandler handler, QueryDefinition... queryDefinitions) {
        synchronized (queryHandlerMonitor) {
            for (QueryDefinition queryDefinition : queryDefinitions) {
                Set<QueryHandler> queryHandlers = this.queryHandlers.computeIfAbsent(queryDefinition.getQueryName(), k -> new CopyOnWriteArraySet<>());
                boolean firstRegistration = supportedQueries.add(queryDefinition);
                queryHandlers.add(handler);
                if (firstRegistration) {
                    doIfNotNull(outboundQueryStream.get(),
                                s -> s.onNext(buildSubscribeMessage(queryDefinition.getQueryName(), queryDefinition.getResultType(), "")));
                }
                logger.info("Registered handler for query {}", queryDefinition);
            }
        }
        return () -> {
            synchronized (queryHandlerMonitor) {
                for (QueryDefinition queryDefinition : queryDefinitions) {
                    Set<QueryHandler> refs = queryHandlers.get(queryDefinition.getQueryName());
                    if (refs != null && refs.remove(handler)) {
                        if (refs.isEmpty()) {
                            queryHandlers.remove(queryDefinition.getQueryName());
                            String instructionId = UUID.randomUUID().toString();
                            outboundQueryStream.get().onNext(
                                    QueryProviderOutbound.newBuilder()
                                                         .setInstructionId(instructionId)
                                                         .setUnsubscribe(QuerySubscription.newBuilder()
                                                                                          .setMessageId(instructionId)
                                                                                          .setQuery(queryDefinition.getQueryName())
                                                                                          .setResultName(queryDefinition.getResultType())
                                                                                          .setClientId(clientIdentification.getClientId())
                                                                                          .setComponentName(clientIdentification.getComponentName()))
                                                         .build()
                            );
                        }
                    }
                }
            }
        };
    }

    @Override
    public ResultStream<QueryResponse> query(QueryRequest query) {
        AbstractBufferedStream<QueryResponse, QueryRequest> results = new AbstractBufferedStream<QueryResponse, QueryRequest>(clientIdentification.getClientId(), Integer.MAX_VALUE, 10) {
            private final QueryResponse TERMINAL = QueryResponse.newBuilder().setErrorCode("__TERMINAL__").build();

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
                // this is a one-way stream. No need to close it.
            }
        };
        queryService.get().query(query, results);
        return results;
    }

    @Override
    public SubscriptionQueryResult subscriptionQuery(QueryRequest query, SerializedObject updateResponseType, int bufferSize, int fetchSize) {
        String subscriptionId = UUID.randomUUID().toString();
        CompletableFuture<QueryResponse> initialResultFuture = new CompletableFuture<>();
        SubscriptionQueryStream subscriptionStream = new SubscriptionQueryStream(subscriptionId, initialResultFuture, QueryChannelImpl.this.clientIdentification.getClientId(), bufferSize, fetchSize);
        StreamObserver<SubscriptionQueryRequest> upstream = queryService.get().subscription(subscriptionStream);
        subscriptionStream.enableFlowControl();
        upstream.onNext(SubscriptionQueryRequest.newBuilder().setSubscribe(SubscriptionQuery.newBuilder().setQueryRequest(query).setSubscriptionIdentifier(subscriptionId).setUpdateResponseType(updateResponseType).build()).build());
        return new SubscriptionQueryResult() {

            private final AtomicBoolean initialResultRequested = new AtomicBoolean();

            @Override
            public CompletableFuture<QueryResponse> initialResult() {
                if (!initialResultFuture.isDone() && !initialResultRequested.getAndSet(true)) {
                    upstream.onNext(SubscriptionQueryRequest.newBuilder().setGetInitialResult(SubscriptionQuery.newBuilder().setQueryRequest(query).setSubscriptionIdentifier(subscriptionId).build()).build());
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
    public void disconnect() {
        doIfNotNull(outboundQueryStream.getAndSet(null), StreamObserver::onCompleted);
    }

    @Override
    public boolean isConnected() {
        return outboundQueryStream.get() != null;
    }

    public void doHandleQuery(QueryProviderInbound query, QueryHandler.ResponseHandler responseHandler) {
        doHandleQuery(query.getQuery(), responseHandler);
    }

    public void doHandleQuery(QueryRequest query, QueryHandler.ResponseHandler responseHandler) {
        Set<QueryHandler> handlers = queryHandlers.getOrDefault(query.getQuery(), Collections.emptySet());
        if (handlers.isEmpty()) {
            responseHandler.sendLastResponse(QueryResponse.newBuilder()
                                                          .setRequestIdentifier(query.getMessageIdentifier())
                                                          .setErrorCode(ErrorCode.NO_HANDLER_FOR_QUERY.errorCode())
                                                          .setErrorMessage(ErrorMessage.newBuilder().setMessage("No handler for query").build())
                                                          .build());
        }
        AtomicInteger completeCounter = new AtomicInteger(handlers.size());
        handlers.forEach(queryHandler -> queryHandler.handle(query, new QueryHandler.ResponseHandler() {
            @Override
            public void sendResponse(QueryResponse response) {
                if (!query.getMessageIdentifier().equals(response.getRequestIdentifier())) {
                    logger.debug("RequestIdentifier not properly set, modifying message");
                    QueryResponse newResponse = response.toBuilder().setRequestIdentifier(query.getMessageIdentifier()).build();
                    responseHandler.sendResponse(newResponse);
                } else {
                    responseHandler.sendResponse(response);
                }
            }

            @Override
            public void complete() {
                if (completeCounter.decrementAndGet() == 0) {
                    responseHandler.complete();
                }
            }
        }));
    }

    private void handleQuery(QueryProviderInbound inbound, ReplyChannel<QueryProviderOutbound> result) {
        doHandleQuery(inbound, new QueryHandler.ResponseHandler() {
            @Override
            public void sendResponse(QueryResponse response) {
                result.send(QueryProviderOutbound.newBuilder().setQueryResponse(response).build());
            }

            @Override
            public void complete() {
                result.markConsumed();
                result.send(QueryProviderOutbound.newBuilder().setQueryComplete(QueryComplete.newBuilder().setRequestId(inbound.getQuery().getMessageIdentifier()).setMessageId(UUID.randomUUID().toString()).build()).build());
            }
        });
    }

    private void getInitialResult(QueryProviderInbound query, ReplyChannel<QueryProviderOutbound> result) {
        String subscriptionId = query.getSubscriptionQueryRequest().getGetInitialResult().getSubscriptionIdentifier();
        doHandleQuery(query.getSubscriptionQueryRequest().getGetInitialResult().getQueryRequest(), new QueryHandler.ResponseHandler() {

            @Override
            public void sendResponse(QueryResponse response) {
                result.send(QueryProviderOutbound.newBuilder()
                                                 .setSubscriptionQueryResponse(SubscriptionQueryResponse.newBuilder()
                                                                                                        .setSubscriptionIdentifier(subscriptionId)
                                                                                                        .setInitialResult(response)
                                                                                                        .setMessageIdentifier(response.getMessageIdentifier())
                                                                                                        .build())
                                                 .build());
            }

            @Override
            public void complete() {
                // no need to send messages here...
            }
        });
    }

    private class IncomingQueryInstructionStream extends AbstractIncomingInstructionStream<QueryProviderInbound, QueryProviderOutbound> {

        public IncomingQueryInstructionStream(String clientId, int permits, int permitsBatch, Consumer<Throwable> disconnectHandler) {
            super(clientId, permits, permitsBatch, disconnectHandler);
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
        protected String getInstructionId(QueryProviderInbound value) {
            return value.getInstructionId();
        }

        @Override
        protected BiConsumer<QueryProviderInbound, ReplyChannel<QueryProviderOutbound>> getHandler(QueryProviderInbound request) {
            if (request.getRequestCase() == QueryProviderInbound.RequestCase.SUBSCRIPTION_QUERY_REQUEST) {
                return instructionHandlers.get(request.getSubscriptionQueryRequest().getRequestCase());
            }
            return instructionHandlers.get(request.getRequestCase());
        }

        @Override
        protected boolean unregisterOutboundStream(StreamObserver<QueryProviderOutbound> expected) {
            return outboundQueryStream.compareAndSet(expected, null);
        }
    }
}
