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

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.connector.command.impl.CommandChannelImpl;
import io.axoniq.axonserver.connector.control.ControlChannel;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.impl.EventChannelImpl;
import io.axoniq.axonserver.connector.query.QueryChannel;
import io.axoniq.axonserver.connector.query.impl.QueryChannelImpl;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.grpc.ConnectivityState;
import io.grpc.StatusRuntimeException;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;

/**
 * Implementation of the {@link AxonServerConnection}, carrying context information with the overall connection. The
 * context information can be used to support the notion of bounded contexts or multi-tenancy for example.
 */
public class ContextConnection implements AxonServerConnection {

    private final ClientIdentification clientIdentification;
    private final ControlChannelImpl controlChannel;
    private final AtomicReference<CommandChannelImpl> commandChannel = new AtomicReference<>();
    private final AtomicReference<EventChannelImpl> eventChannel = new AtomicReference<>();
    private final AtomicReference<QueryChannelImpl> queryChannel = new AtomicReference<>();
    private final ScheduledExecutorService executorService;
    private final AxonServerManagedChannel connection;
    private final int commandPermits;
    private final int queryPermits;
    private final String context;
    private final Consumer<ContextConnection> onShutdown;

    /**
     * Construct a {@link ContextConnection} carrying context information.
     *
     * @param clientIdentification         client information identifying whom has connected. This information is used
     *                                     to pass on to message
     * @param executorService              a {@link ScheduledExecutorService} used to schedule reconnects in the
     *                                     channels this connection provides
     * @param connection                   the {@link AxonServerManagedChannel} used to form the connections with
     *                                     AxonServer
     * @param processorInfoUpdateFrequency the update frequency in milliseconds of event processor information
     * @param commandPermits               the number of permits for command streams
     * @param queryPermits                 the number of permits for query streams
     * @param context                      the context this connection belongs to
     */
    public ContextConnection(ClientIdentification clientIdentification,
                             ScheduledExecutorService executorService,
                             AxonServerManagedChannel connection,
                             long processorInfoUpdateFrequency,
                             int commandPermits,
                             int queryPermits,
                             String context,
                             Consumer<ContextConnection> onShutdown) {
        this.clientIdentification = clientIdentification;
        this.executorService = executorService;
        this.connection = connection;
        this.commandPermits = commandPermits;
        this.queryPermits = queryPermits;
        this.context = context;
        this.onShutdown = onShutdown;
        this.controlChannel = new ControlChannelImpl(clientIdentification,
                                                     context,
                                                     executorService,
                                                     connection,
                                                     processorInfoUpdateFrequency,
                                                     this::reconnectChannels);
    }

    private void reconnectChannels() {
        connection.requestReconnect();
        doIfNotNull(commandChannel.get(), CommandChannelImpl::reconnect);
        doIfNotNull(queryChannel.get(), QueryChannelImpl::reconnect);
        doIfNotNull(controlChannel, ControlChannelImpl::reconnect);
        doIfNotNull(eventChannel.get(), EventChannelImpl::reconnect);
    }

    @Override
    public boolean isConnectionFailed() {
        return connection.getState(false) == ConnectivityState.TRANSIENT_FAILURE;
    }

    @Override
    public boolean isReady() {
        return isConnected()
                && Optional.ofNullable(commandChannel.get()).map(CommandChannelImpl::isReady).orElse(true)
                && Optional.ofNullable(queryChannel.get()).map(QueryChannelImpl::isReady).orElse(true)
                && Optional.ofNullable(eventChannel.get()).map(EventChannelImpl::isReady).orElse(true)
                && controlChannel.isReady();
    }

    @Override
    public boolean isConnected() {
        return connection.getState(false) == ConnectivityState.READY;
    }

    @Override
    public void disconnect() {
        doIfNotNull(controlChannel, ControlChannelImpl::disconnect);
        doIfNotNull(commandChannel.get(), CommandChannelImpl::disconnect);
        doIfNotNull(queryChannel.get(), QueryChannelImpl::disconnect);
        doIfNotNull(eventChannel.get(), EventChannelImpl::disconnect);
        connection.shutdown();
        onShutdown.accept(this);
        try {
            if (!connection.awaitTermination(5, TimeUnit.SECONDS)) {
                connection.shutdownNow();
            }
        } catch (InterruptedException e) {
            connection.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public ControlChannel controlChannel() {
        return controlChannel;
    }

    /**
     * Ensure there is a connection with the {@link ControlChannel} this connection provides.
     */
    public void connect() {
        ensureConnected(controlChannel);
    }

    @Override
    public CommandChannel commandChannel() {
        CommandChannelImpl channel = this.commandChannel.updateAndGet(
                createIfNull(() -> new CommandChannelImpl(clientIdentification, context, commandPermits, commandPermits / 4, executorService, connection))
        );
        return ensureConnected(channel);
    }

    @Override
    public EventChannel eventChannel() {
        EventChannelImpl channel = this.eventChannel.updateAndGet(
                createIfNull(() -> new EventChannelImpl(clientIdentification, executorService, connection))
        );
        return ensureConnected(channel);
    }

    @Override
    public QueryChannel queryChannel() {
        QueryChannelImpl channel = this.queryChannel.updateAndGet(
                createIfNull(() -> new QueryChannelImpl(clientIdentification, context, queryPermits, queryPermits / 4, executorService, connection))
        );
        return ensureConnected(channel);
    }

    private <T> UnaryOperator<T> createIfNull(Supplier<T> factory) {
        return existing -> existing == null ? factory.get() : existing;
    }

    private <T extends AbstractAxonServerChannel> T ensureConnected(T channel) {
        if (!channel.isReady()) {
            ConnectivityState state = connection.getState(true);
            if (state != ConnectivityState.SHUTDOWN && state != ConnectivityState.TRANSIENT_FAILURE) {
                try {
                    channel.connect();
                } catch (StatusRuntimeException e) {
                    connection.notifyWhenStateChanged(state, channel::connect);
                }
            } else {
                connection.notifyWhenStateChanged(state, channel::connect);
            }
        }
        return channel;
    }

    /**
     * Retrieve the {@link AxonServerManagedChannel} used to create connection with.
     *
     * @return the {@link AxonServerManagedChannel} used to create connection with
     */
    public AxonServerManagedChannel getManagedChannel() {
        return connection;
    }
}
