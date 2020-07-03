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
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;

public class ContextConnection implements AxonServerConnection {

    private final ClientIdentification clientIdentification;
    private final ControlChannelImpl controlChannel;
    private final AtomicReference<CommandChannelImpl> commandChannel = new AtomicReference<>();
    private final AtomicReference<EventChannelImpl> eventChannel = new AtomicReference<>();
    private final AtomicReference<QueryChannelImpl> queryChannel = new AtomicReference<>();
    private final ScheduledExecutorService executorService;
    private final AxonServerManagedChannel connection;

    public ContextConnection(ClientIdentification clientIdentification,
                             ScheduledExecutorService executorService,
                             AxonServerManagedChannel connection,
                             String context) {
        this.clientIdentification = clientIdentification;
        this.executorService = executorService;
        this.connection = connection;
        this.controlChannel = new ControlChannelImpl(clientIdentification, context, executorService, connection);
    }

    @Override
    public boolean isConnectionFailed() {
        return connection.getState(false) == ConnectivityState.TRANSIENT_FAILURE;
    }

    @Override
    public boolean isReady() {
        return isConnected()
                && Optional.ofNullable(commandChannel.get()).map(CommandChannelImpl::isConnected).orElse(true)
                && Optional.ofNullable(queryChannel.get()).map(QueryChannelImpl::isConnected).orElse(true)
                && Optional.ofNullable(eventChannel.get()).map(EventChannelImpl::isConnected).orElse(true)
                && controlChannel.isConnected();
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

    @Override
    public CommandChannel commandChannel() {
        CommandChannelImpl channel = this.commandChannel.updateAndGet(getIfNull(() -> new CommandChannelImpl(clientIdentification, 5000, 2000, executorService, connection)));
        return ensureConnected(channel);
    }

    @Override
    public EventChannel eventChannel() {
        EventChannelImpl channel = this.eventChannel.updateAndGet(getIfNull(() -> new EventChannelImpl(executorService, connection)));
        return ensureConnected(channel);
    }

    @Override
    public QueryChannel queryChannel() {
        QueryChannelImpl channel = this.queryChannel.updateAndGet(getIfNull(() -> new QueryChannelImpl(clientIdentification, 5000, 2000, executorService, connection)));
        return ensureConnected(channel);
    }

    private <T extends AbstractAxonServerChannel> T ensureConnected(T channel) {
        if (!channel.isConnected()) {
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

    private <T> UnaryOperator<T> getIfNull(Supplier<T> factory) {
        return existing -> existing == null ? factory.get() : existing;
    }

    public void connect() {
        ensureConnected(controlChannel);
    }

}
