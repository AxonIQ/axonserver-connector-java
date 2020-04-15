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

package io.axoniq.axonserver.connector;

import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static io.axoniq.axonserver.connector.ObjectUtils.silently;

public class ContextConnection {

    private static final Logger logger = LoggerFactory.getLogger(ContextConnection.class);
    private final String context;
    private final ClientIdentification clientIdentification;
    private final AtomicReference<InstructionChannel> instructionChannel = new AtomicReference<>();
    private final AtomicReference<CommandChannel> commandChannel = new AtomicReference<>();
    private final AtomicReference<ManagedChannel> connection = new AtomicReference<>();
    private ScheduledExecutorService executorService;
    private BiFunction<String, ClientIdentification, ManagedChannel> connectionFactory;

    public ContextConnection(String context, ClientIdentification clientIdentification,
                             ScheduledExecutorService executorService,
                             BiFunction<String, ClientIdentification, ManagedChannel> connectionFactory) {
        this.context = context;
        this.clientIdentification = clientIdentification;
        this.executorService = executorService;
        this.connectionFactory = connectionFactory;
    }

    public boolean isConnected() {
        return
                Optional.ofNullable(connection.get()).map(c -> c.getState(false) == ConnectivityState.READY
                        && (Optional.ofNullable(instructionChannel.get()).map(InstructionChannel::isConnected).orElse(false)
                        || Optional.ofNullable(commandChannel.get()).map(CommandChannel::isConnected).orElse(false))
                ).orElse(false);
        // TODO - Also check command, query and event context
    }

    public void disconnect() {
        ManagedChannel c = connection.getAndSet(null);
        c.shutdown();
        ObjectUtils.doIfNotNull(instructionChannel.get(), InstructionChannel::disconnect);
        ObjectUtils.doIfNotNull(commandChannel.get(), CommandChannel::disconnect);
        try {
            if (!c.awaitTermination(5, TimeUnit.SECONDS)) {
                c.shutdownNow();
            }
        } catch (InterruptedException e) {
            c.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public InstructionChannel instructionChannel() {
        InstructionChannel instructionChannel = this.instructionChannel.updateAndGet(getIfNull(() -> new InstructionChannel(clientIdentification)));
        if (!instructionChannel.isConnected()) {
            instructionChannel.connect(getLiveConnection());
        }
        return instructionChannel;
    }

    public CommandChannel commandChannel() {
        CommandChannel commandChannel = this.commandChannel.updateAndGet(getIfNull(() -> new CommandChannel(clientIdentification, 5000, 2000)));
        if (!commandChannel.isConnected()) {
            commandChannel.connect(getLiveConnection());
        }
        return commandChannel;
    }

    private ManagedChannel getLiveConnection() {
        ManagedChannel existing = connection.get();
        if (existing == null || existing.isShutdown() || existing.getState(false) == ConnectivityState.TRANSIENT_FAILURE) {
            logger.info("Setting up new connection to AxonServer");
            synchronized (connection) {
                existing = connection.get();
                if (existing == null || existing.isShutdown() || existing.getState(false) == ConnectivityState.TRANSIENT_FAILURE) {
                    ManagedChannel newChannel = connectionFactory.apply(context, clientIdentification);
                    connection.getAndSet(newChannel);
                    silently(existing, ManagedChannel::shutdown);
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
            logger.info("Connection lost. Reconnecting...");
            existing.shutdown();
            try {
                ManagedChannel newConnection = connectionFactory.apply(context, clientIdentification);
                connection.compareAndSet(existing, newConnection);
                ObjectUtils.doIfNotNull(instructionChannel.get(), i -> i.connect(newConnection));
                ObjectUtils.doIfNotNull(commandChannel.get(), i -> i.connect(newConnection));
            } catch (Exception e) {
                logger.info("Failed to get connection to AxonServer. Scheduling a reconnect");
                executorService.schedule(this::checkConnection, 1, TimeUnit.SECONDS);
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

    // TODO - Create EventChannel
//    public EventChannel eventChannel() {
//
//    }

}
