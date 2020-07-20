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
import io.grpc.StatusRuntimeException;
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
import javax.annotation.Nullable;

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
    private final AtomicReference<Exception> lastConnectException = new AtomicReference<>();

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
        this.executor.schedule(() -> ensureConnected(true), 100, TimeUnit.MILLISECONDS);
    }

    private ManagedChannel connectChannel() {
        ManagedChannel connection = null;
        for (ServerAddress nodeInfo : routingServers) {
            ManagedChannel candidate = connectionFactory.apply(nodeInfo, context);
            try {
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
            } catch (StatusRuntimeException sre) {
                lastConnectException.set(sre);
                shutdownNow(candidate);
                if (!suppressErrors.getAndSet(true)) {
                    logger.warn("Connecting to AxonServer node [{}] failed.", nodeInfo, sre);
                } else {
                    logger.warn("Connecting to AxonServer node [{}] failed: {}", nodeInfo, sre.getMessage());
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
        if (current == null || current.getState(false) != ConnectivityState.READY) {
            ensureConnected(false);
            current = activeChannel.get();
        }
        if (current == null) {
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
            ensureConnected(false);
        }
        ManagedChannel current = activeChannel.get();
        if (current == null) {
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

    private void ensureConnected(boolean allowReschedule) {
        if (shutdown.get()) {
            return;
        }

        long now = System.currentTimeMillis();
        long deadline = nextAttemptTime.getAndUpdate(d -> d > now ? d : now + reconnectInterval);
        if (deadline > now) {
            if (allowReschedule) {
                long timeLeft = Math.min(500, deadline - now);
                logger.debug("Reconnect timeout still enforced. Scheduling a new connection check in {}ms", timeLeft);
                scheduleConnectionCheck(timeLeft);
            }
            return;
        }
        logger.debug("Checking connection state");
        ManagedChannel current = activeChannel.get();

        ConnectivityState state = current == null ? ConnectivityState.SHUTDOWN : current.getState(true);
        if (state == ConnectivityState.TRANSIENT_FAILURE || state == ConnectivityState.SHUTDOWN) {
            if (state == ConnectivityState.TRANSIENT_FAILURE) {
                logger.info("Connection to AxonServer lost. Attempting to reconnect...");
            }
            createConnection(allowReschedule, current);
        } else if (allowReschedule && (state == ConnectivityState.IDLE || state == ConnectivityState.CONNECTING)) {
            logger.debug("Connection is {}, checking again in 50ms", state);
            scheduleConnectionCheck(50);
        } else {
            logger.debug("Connection seems normal. {}", state);
            if (allowReschedule) {
                logger.debug("Registering state change handler");
                current.notifyWhenStateChanged(ConnectivityState.READY, this::verifyConnectionStateChange);
            }
        }
    }

    private void createConnection(boolean allowReschedule, ManagedChannel current) {
        ManagedChannel newConnection = null;
        try {
            if (forcePlatformReconnect && current != null && !current.isShutdown()) {
                logger.debug("Shut down current connection");
                current.shutdown();
            }
            newConnection = connectChannel();
            if (newConnection != null) {
                if (!activeChannel.compareAndSet(current, newConnection)) {
                    // concurrency. We need to abandon the given connection
                    logger.debug("A successful Connection was concurrently set up. Closing this one.");
                    newConnection.shutdown();
                    if (allowReschedule) {
                        // we need to make sure the connection checked "process" is always kept alive
                        scheduleConnectionCheck(50);
                    }
                    return;
                }
                doIfNotNull(current, ManagedChannel::shutdown);

                if (logger.isInfoEnabled()) {
                    logger.info("Successfully connected to {}", newConnection.authority());
                }
                Runnable listener;
                while ((listener = connectListeners.poll()) != null) {
                    listener.run();
                }
                nextAttemptTime.set(0);
                if (allowReschedule) {
                    logger.debug("Registering state change handler");
                    newConnection.notifyWhenStateChanged(ConnectivityState.READY, this::verifyConnectionStateChange);
                }
            } else if (allowReschedule) {
                logger.info("Failed to get connection to AxonServer. Scheduling a reconnect in {}ms",
                            reconnectInterval);
                scheduleConnectionCheck(reconnectInterval);
            }
        } catch (Exception e) {
            doIfNotNull(newConnection, ManagedChannel::shutdown);
            if (allowReschedule) {
                logger.info("Failed to get connection to AxonServer. Scheduling a reconnect in {}ms",
                            reconnectInterval, e);
                scheduleConnectionCheck(reconnectInterval);
            } else {
                logger.debug("Failed to get connection to AxonServer.");
            }
        }
    }

    private void verifyConnectionStateChange() {
        logger.debug("Connection state changed. Checking state now...");
        scheduleConnectionCheck(0);
    }

    private void scheduleConnectionCheck(long interval) {
        try {
            executor.schedule(() -> ensureConnected(true), interval, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            logger.info("Did not schedule reconnect attempt. Connector is shut down");
        }
    }

    /**
     * Requests a dedicated reconnect of this {@link ManagedChannel} implementation.
     */
    public void requestReconnect() {
        logger.info("Reconnect requested. Closing current connection");
        doIfNotNull(activeChannel.get(), c -> {
            c.shutdown();
            executor.schedule(c::shutdownNow, 5, TimeUnit.SECONDS);
        });
    }

    /**
     * Forcefully perform a reconnect of this {@link ManagedChannel} implementation by shutting down the current
     * connection.
     */
    public void forceReconnect() {
        logger.info("Forceful reconnect required. Closing current connection");
        ManagedChannel currentChannel = activeChannel.get();
        if (currentChannel != null) {
            currentChannel.shutdown();
            try {
                currentChannel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            currentChannel.shutdownNow();
        }
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
        public void cancel(@Nullable String message, @Nullable Throwable cause) {
            // great
        }

        @Override
        public void halfClose() {
            // great
        }

        @Override
        public void sendMessage(REQ message) {
            throw new StatusRuntimeException(Status.UNAVAILABLE);
        }
    }
}
