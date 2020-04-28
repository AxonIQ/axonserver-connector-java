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
import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.connector.command.impl.CommandChannelImpl;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.impl.EventChannelImpl;
import io.axoniq.axonserver.connector.instruction.InstructionChannel;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;

public class ContextConnection implements AxonServerConnection {

    private static final Logger logger = LoggerFactory.getLogger(ContextConnection.class);
    private final String context;
    private final ClientIdentification clientIdentification;
    private final InstructionChannelImpl instructionChannel;
    private final AtomicReference<CommandChannelImpl> commandChannel = new AtomicReference<>();
    private final AtomicReference<ManagedChannel> connection = new AtomicReference<>();
    private final AtomicReference<EventChannelImpl> eventChannel = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> connectTask = new AtomicReference<>();
    private final ScheduledExecutorService executorService;
    private final BiFunction<String, ClientIdentification, ManagedChannel> connectionFactory;

    public ContextConnection(String context, ClientIdentification clientIdentification,
                             ScheduledExecutorService executorService,
                             BiFunction<String, ClientIdentification, ManagedChannel> connectionFactory) {
        this.context = context;
        this.clientIdentification = clientIdentification;
        this.executorService = executorService;
        this.connectionFactory = connectionFactory;
        this.instructionChannel = new InstructionChannelImpl(clientIdentification, executorService);
    }

    @Override
    public boolean isConnected() {
        return
                Optional.ofNullable(connection.get()).map(c -> c.getState(false) == ConnectivityState.READY).orElse(false)
                        && instructionChannel.isConnected();
    }

    @Override
    public void disconnect() {
        ManagedChannel channel = connection.getAndSet(null);
        doIfNotNull(channel, ManagedChannel::shutdown);
        doIfNotNull(instructionChannel, InstructionChannelImpl::disconnect);
        doIfNotNull(commandChannel.get(), CommandChannelImpl::disconnect);
        doIfNotNull(connectTask.get(), c -> c.cancel(false));
        if (channel != null) {
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public InstructionChannel instructionChannel() {
        return connected(instructionChannel);
    }

    @Override
    public CommandChannel commandChannel() {
        CommandChannelImpl commandChannel = this.commandChannel.updateAndGet(getIfNull(() -> new CommandChannelImpl(clientIdentification, 5000, 2000, executorService)));
        return connected(commandChannel);
    }

    @Override
    public EventChannel eventChannel() {
        EventChannelImpl eventChannel = this.eventChannel.updateAndGet(getIfNull(() -> new EventChannelImpl(executorService)));
        return connected(eventChannel);
    }

    private <T extends AbstractAxonServerChannel> T connected(T channel) {
        if (!channel.isConnected()) {
            ManagedChannel connection = getLiveConnection();
            ConnectivityState state = connection.getState(false);
            if (state != ConnectivityState.SHUTDOWN && state != ConnectivityState.TRANSIENT_FAILURE) {
                channel.connect(connection);
            }
        }
        return channel;
    }

    private ManagedChannel getLiveConnection() {
        ManagedChannel existing = connection.get();
        if (existing == null) {
            logger.info("Setting up new connection to AxonServer");
            synchronized (connection) {
                existing = connection.get();
                if (existing == null) {
                    ManagedChannel newChannel;
                    try {
                        newChannel = connectionFactory.apply(context, clientIdentification);
                    } catch (AxonServerException ex) {
                        newChannel = new DisconnectedChannel();
                    }
                    connection.set(newChannel);
                    newChannel.notifyWhenStateChanged(ConnectivityState.READY, this::checkConnection);
                    return newChannel;
                }
            }
        }
        logger.info("Reusing existing connection to AxonServer");
        return existing;
    }

    private void checkConnection() {
        ManagedChannel existing = connection.get();
        if (existing == null) {
            logger.info("Disconnected from AxonServer");
            return;
        }
        logger.info("Checking connection state");
        ConnectivityState state = existing.getState(true);
        if (state == ConnectivityState.TRANSIENT_FAILURE || state == ConnectivityState.SHUTDOWN) {
            logger.info("No active connection available. Attempting to reconnect...");
            existing.shutdown();
            try {
                ManagedChannel newConnection = connectionFactory.apply(context, clientIdentification);
                connection.compareAndSet(existing, newConnection);
                doIfNotNull(instructionChannel, i -> i.connect(newConnection));
                doIfNotNull(commandChannel.get(), i -> i.connect(newConnection));
                newConnection.notifyWhenStateChanged(ConnectivityState.READY, this::checkConnection);
            } catch (Exception e) {
                logger.info("Failed to get connection to AxonServer. Scheduling a reconnect");
                ScheduledFuture<?> previous = connectTask.getAndSet(
                        executorService.schedule(this::checkConnection, 5, TimeUnit.SECONDS));
                doIfNotNull(previous, p -> p.cancel(false));
            }
        } else {
            logger.info("Connection seems normal. {}" , state.name());
            existing.notifyWhenStateChanged(state, this::checkConnection);
        }
    }

    private <T> UnaryOperator<T> getIfNull(Supplier<T> factory) {
        return existing -> existing == null ? factory.get() : existing;
    }

    // TODO - Create QueryChannel
//    public QueryChannel queryChannel() {
//
//    }

}
