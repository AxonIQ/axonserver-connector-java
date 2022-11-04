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

import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.NodeInfo;
import io.axoniq.axonserver.grpc.control.PlatformInfo;
import io.axoniq.axonserver.grpc.control.PlatformServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;

/**
 * AxonServer specific {@link ManagedChannel} implementation providing AxonServer specific connection logic.
 */
public class AxonServerManagedChannel extends ManagedChannel {

    private static final Logger logger = LoggerFactory.getLogger(AxonServerManagedChannel.class);

    private final List<ServerAddress> routingServers;
    private final long reconnectInterval;
    private final String context;
    private final ClientIdentification clientIdentification;
    private final ScheduledExecutorService executor;
    private final boolean forcePlatformReconnect;
    private final BiFunction<ServerAddress, String, ManagedChannel> connectionFactory;
    private final long connectTimeout;

    private final AtomicReference<ManagedChannel> activeChannel = new AtomicReference<>();
    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final AtomicBoolean suppressErrors = new AtomicBoolean();
    private final Queue<Runnable> connectListeners = new LinkedBlockingQueue<>();
    private final AtomicLong nextAttemptTime = new AtomicLong();
    private final AtomicLong connectionDeadline = new AtomicLong();
    private final AtomicReference<Exception> lastConnectException = new AtomicReference<>();
    private final AtomicBoolean scheduleGate = new AtomicBoolean();

    /**
     * Constructs a {@link AxonServerManagedChannel}.
     *
     * @param routingServers         {@link List} of {@link ServerAddress}' denoting the instances to connect with
     * @param reconnectConfiguration configuration holder defining how this {@link ManagedChannel} implementation should
     *                               reconnect
     * @param context                the context this {@link ManagedChannel} operates in
     * @param clientIdentification   the information identifying the client application which is connecting. This
     *                               information is used to form the connection with a client
     * @param executor               {@link ScheduledExecutorService} used to schedule operations to ensure the
     *                               connection is maintained
     * @param connectionFactory      factory method able of creating new {@link ManagedChannel} instances based on a
     *                               given {@link ServerAddress} and context
     */
    public AxonServerManagedChannel(List<ServerAddress> routingServers,
                                    ReconnectConfiguration reconnectConfiguration,
                                    String context,
                                    ClientIdentification clientIdentification,
                                    ScheduledExecutorService executor,
                                    BiFunction<ServerAddress, String, ManagedChannel> connectionFactory) {
        this.routingServers = new ArrayList<>(routingServers);
        this.reconnectInterval = reconnectConfiguration.getTimeUnit()
                                                       .toMillis(reconnectConfiguration.getReconnectInterval());
        this.context = context;
        this.clientIdentification = clientIdentification;
        this.executor = executor;
        this.forcePlatformReconnect = reconnectConfiguration.isForcePlatformReconnect();
        this.connectionFactory = connectionFactory;
        this.connectTimeout = reconnectConfiguration.getTimeUnit().toMillis(reconnectConfiguration.getConnectTimeout());
    }

    private ManagedChannel connectChannel() {
        ManagedChannel connection = null;
        for (ServerAddress nodeInfo : routingServers) {
            ManagedChannel candidate = null;
            try {
                candidate = connectionFactory.apply(nodeInfo, context);
                PlatformServiceGrpc.PlatformServiceBlockingStub stub =
                        PlatformServiceGrpc.newBlockingStub(candidate)
                                           .withDeadlineAfter(connectTimeout, TimeUnit.MILLISECONDS);
                logger.info("Requesting connection details from {}:{}",
                            nodeInfo.getHostName(), nodeInfo.getGrpcPort());

                PlatformInfo clusterInfo = stub.getPlatformServer(clientIdentification);
                NodeInfo primaryClusterInfo = clusterInfo.getPrimary();
                logger.debug("Received PlatformInfo suggesting [{}] ({}:{}), {}",
                             primaryClusterInfo.getNodeName(),
                             primaryClusterInfo.getHostName(),
                             primaryClusterInfo.getGrpcPort(),
                             clusterInfo.getSameConnection()
                                     ? "allowing use of existing connection"
                                     : "requiring new connection");
                if (clusterInfo.getSameConnection()
                        || (primaryClusterInfo.getGrpcPort() == nodeInfo.getGrpcPort()
                        && primaryClusterInfo.getHostName().equals(nodeInfo.getHostName()))) {
                    logger.debug("Reusing existing channel");
                    connection = candidate;
                } else {
                    candidate.shutdown();
                    logger.info("Connecting to [{}] ({}:{})",
                                primaryClusterInfo.getNodeName(),
                                primaryClusterInfo.getHostName(),
                                primaryClusterInfo.getGrpcPort());
                    ServerAddress serverAddress =
                            new ServerAddress(primaryClusterInfo.getHostName(), primaryClusterInfo.getGrpcPort());
                    connection = connectionFactory.apply(serverAddress, context);
                }
                suppressErrors.set(false);
                lastConnectException.set(null);
                break;
            } catch (Exception e) {
                lastConnectException.set(e);
                doIfNotNull(candidate, this::shutdownNow);
                if (!suppressErrors.getAndSet(true)) {
                    logger.warn("Connecting to AxonServer node [{}] failed.", nodeInfo, e);
                } else {
                    logger.warn("Connecting to AxonServer node [{}] failed: {}", nodeInfo, e.getMessage());
                }
            }
        }
        return connection;
    }

    private void shutdownNow(ManagedChannel managedChannel) {
        try {
            managedChannel.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Interrupted during shutdown");
        }
    }

    @Override
    public ManagedChannel shutdown() {
        shutdown.set(true);
        doIfNotNull(activeChannel.get(), ManagedChannel::shutdown);
        return this;
    }

    @Override
    public boolean isShutdown() {
        return shutdown.get();
    }

    @Override
    public boolean isTerminated() {
        if (!shutdown.get()) {
            return false;
        }
        ManagedChannel current = this.activeChannel.get();
        return current == null || current.isTerminated();
    }

    @Override
    public ManagedChannel shutdownNow() {
        shutdown.set(true);
        doIfNotNull(activeChannel.get(), ManagedChannel::shutdownNow);
        return this;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        ManagedChannel current = activeChannel.get();
        if (current != null) {
            current.awaitTermination(timeout, unit);
        }
        return true;
    }

    @Override
    public <REQ, RESP> ClientCall<REQ, RESP> newCall(MethodDescriptor<REQ, RESP> methodDescriptor,
                                                     CallOptions callOptions) {
        ManagedChannel current = activeChannel.get();
        if (current == null || current.isShutdown() || current.getState(false) != ConnectivityState.READY) {
            ensureConnected();
            current = activeChannel.get();
        }
        if (current == null || current.isShutdown()) {
            return new FailingCall<>();
        }
        return current.newCall(methodDescriptor, callOptions);
    }

    @Override
    public String authority() {
        return routingServers.get(0).toString();
    }

    @Override
    public ConnectivityState getState(boolean requestConnection) {
        if (shutdown.get()) {
            return ConnectivityState.SHUTDOWN;
        }

        if (requestConnection) {
            ensureConnected();
        }
        ManagedChannel current = activeChannel.get();
        if (current == null || current.isShutdown()) {
            if (lastConnectException.get() == null) {
                return ConnectivityState.IDLE;
            } else {
                return ConnectivityState.TRANSIENT_FAILURE;
            }
        }
        ConnectivityState state = current.getState(requestConnection);
        return state == ConnectivityState.SHUTDOWN ? ConnectivityState.TRANSIENT_FAILURE : state;
    }

    @Override
    public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {
        ManagedChannel current = activeChannel.get();
        logger.debug("Registering state change listener for {} on channel {}", source, current);
        switch (source) {
            case SHUTDOWN:
            case READY:
            case IDLE:
            case CONNECTING:
                if (current != null) {
                    current.notifyWhenStateChanged(source, callback);
                } else {
                    callback.run();
                }
                break;
            case TRANSIENT_FAILURE:
                if (current == null) {
                    connectListeners.add(callback);
                } else {
                    callback.run();
                }
                break;
        }
    }

    @Override
    public void resetConnectBackoff() {
        doIfNotNull(activeChannel.get(), ManagedChannel::resetConnectBackoff);
    }

    @Override
    public void enterIdle() {
        doIfNotNull(activeChannel.get(), ManagedChannel::enterIdle);
    }

    private void ensureConnected() {
        if (shutdown.get()) {
            return;
        }

        logger.debug("Checking connection state");
        ManagedChannel current = activeChannel.get();

        ConnectivityState state = current == null ? ConnectivityState.SHUTDOWN : current.getState(true);
        long now = System.currentTimeMillis();
        switch (state) {
            case TRANSIENT_FAILURE:
            case SHUTDOWN:
                long deadline = nextAttemptTime.getAndUpdate(d -> d > now ? d : now + reconnectInterval);
                if (deadline > now) {
                    long timeLeft = Math.min(500, deadline - now);
                    logger.debug("Reconnect timeout still enforced. Scheduling a new connection check in {}ms", timeLeft);
                    scheduleConnectionCheck(timeLeft);
                    return;
                }

                if (current != null) {
                    logger.info("Connection to AxonServer lost. Attempting to reconnect...");
                }
                createConnection(current);
                break;
            case CONNECTING:
                long connectDeadline = connectionDeadline.getAndUpdate(d -> d > now ? d : now + connectTimeout);
                // if connectDeadline == 0, then this is the first time we're in CONNECTING state. We should give that
                // connection a chance. If it's not 0, then a deadline has been set.
                if (connectDeadline != 0 && connectDeadline < now) {
                    logger.info("Unable to recover current connection to AxonServer. Attempting to reconnect...");
                    createConnection(current);
                } else {
                    scheduleConnectionCheck(Math.min(500, connectDeadline - now));
                }
                break;
            case READY:
                if (forcePlatformReconnect) {
                    // by forcing a non-0 value, it will reconnect automatically when the connection switches to
                    // CONNECTING state from here. When forcePlatformReconnect is enabled, we want this more aggressive
                    // reconnection
                    connectionDeadline.set(1);
                }
                logger.debug("Connection is {}", state);
                break;
            case IDLE:
            default:
                logger.debug("Connection is {}, checking again in 50ms", state);
                scheduleConnectionCheck(50);
                break;
        }
    }

    private void createConnection(ManagedChannel current) {
        if (forcePlatformReconnect && current != null && !current.isShutdown()) {
            logger.debug("Shut down current connection");
            current.shutdown();
        }
        ManagedChannel newConnection = connectChannel();
        if (newConnection != null) {
            if (!activeChannel.compareAndSet(current, newConnection)) {
                // concurrency. We need to abandon the given connection
                logger.debug("A successful Connection was concurrently set up. Closing this one.");
                newConnection.shutdown();
                return;
            }
            doIfNotNull(current, ManagedChannel::shutdown);

            if (logger.isInfoEnabled()) {
                logger.info("Successfully connected to {}", newConnection.authority());
            }
            // a new connection, so deadlines should be reset
            connectionDeadline.set(0);
            nextAttemptTime.set(0);
            logger.debug("Registering state change handler");
            newConnection.notifyWhenStateChanged(ConnectivityState.READY, () -> verifyConnectionStateChange(newConnection));
            Runnable listener;
            while ((listener = connectListeners.poll()) != null) {
                listener.run();
            }
        } else {
            logger.info("Failed to get connection to AxonServer. Scheduling a reconnect in {}ms", reconnectInterval);
            scheduleConnectionCheck(reconnectInterval);
        }
    }

    private void verifyConnectionStateChange(ManagedChannel channel) {
        ConnectivityState currentState = channel.getState(false);
        logger.debug("Connection state changed to {} scheduling connection check.", currentState);
        if (currentState != ConnectivityState.SHUTDOWN) {
            logger.debug("Registering new state change handler");
            channel.notifyWhenStateChanged(currentState, () -> verifyConnectionStateChange(channel));
        }
        scheduleConnectionCheck(10);
    }

    private void scheduleConnectionCheck(long interval) {
        try {
            if (scheduleGate.compareAndSet(false, true)) {
                executor.schedule(() -> {
                    scheduleGate.set(false);
                    ensureConnected();
                }, interval, TimeUnit.MILLISECONDS);
            }
        } catch (RejectedExecutionException e) {
            scheduleGate.set(false);
            logger.debug("Did not schedule reconnect attempt. Connector is shut down");
        }
    }

    /**
     * Requests a dedicated reconnect of this {@link ManagedChannel} implementation.
     */
    public void requestReconnect() {
        doIfNotNull(this.activeChannel.getAndSet(null), currentChannel -> {
            logger.info("Reconnect for context {} requested. Closing current connection.", context);
            nextAttemptTime.set(0);
            currentChannel.shutdown();
            executor.schedule(currentChannel::shutdownNow, 5, TimeUnit.SECONDS);
        });
    }

    /**
     * Indicates whether the channel is READY, meaning is is connected to an Axon Server instance,
     * and ready to accept calls.
     *
     * @return {@code true} if the connection is ready to accept calls, otherwise {@code false}
     */
    public boolean isReady() {
        return getState(false) == ConnectivityState.READY;
    }

    private static class FailingCall<REQ, RESP> extends ClientCall<REQ, RESP> {

        @Override
        public void start(Listener<RESP> responseListener, Metadata headers) {
            responseListener.onClose(Status.UNAVAILABLE, null);
        }

        @Override
        public void request(int numMessages) {
            // ignore
        }

        @Override
        public void cancel(String message, Throwable cause) {
            // great
        }

        @Override
        public void halfClose() {
            // great
        }

        @Override
        public void sendMessage(REQ message) {
            // ignore these messages. The returning stream has already given an error
        }
    }
}
