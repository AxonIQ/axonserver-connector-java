/*
 * Copyright (c) 2020. AxonIQ
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

import io.grpc.ConnectivityState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class representing a channel with AxonServer.
 */
public abstract class AbstractAxonServerChannel {

    private static final Logger logger = LoggerFactory.getLogger(AbstractAxonServerChannel.class);

    private final ScheduledExecutorService executor;
    private final AxonServerManagedChannel channel;

    /**
     * Instantiate an {@link AbstractAxonServerChannel}.
     *
     * @param executor                 a {@link ScheduledExecutorService} used to invoke {@link #scheduleReconnect()} in
     *                                 a separate thread
     * @param axonServerManagedChannel the {@link AxonServerManagedChannel} used to validate whether {@link
     *                                 #scheduleReconnect()} is succeeded or should be tried again
     */
    public AbstractAxonServerChannel(ScheduledExecutorService executor,
                                     AxonServerManagedChannel axonServerManagedChannel) {
        this.executor = executor;
        this.channel = axonServerManagedChannel;
    }

    /**
     * Schedule an attempt to reconnect with AxonServer.
     */
    protected void scheduleReconnect() {
        scheduleReconnect(false);
    }

    /**
     * Schedule an immediate attempt to reconnect with AxonServer.
     */
    protected void scheduleImmediateReconnect() {
        logger.debug("Scheduling immediate reconnect");
        scheduleReconnect(true);
    }

    private void scheduleReconnect(boolean immediate) {
        try {
            executor.schedule(() -> {
                ConnectivityState connectivityState = channel.getState(immediate);
                if (connectivityState == ConnectivityState.READY) {
                    connect();
                } else {
                    logger.debug("No connection to AxonServer available. Scheduling next attempt in 500ms");
                    scheduleReconnect(false);
                }
            }, immediate ? 0 : 500, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            logger.info("Ignoring reconnect request, as connector is being thus down.");
        }
    }

    /**
     * Connect this channel with AxonServer.
     */
    public abstract void connect();

    /**
     * Reconnect this channel with AxonServer.
     */
    public abstract void reconnect();

    /**
     * Disconnect this channel from AxonServer.
     */
    public abstract void disconnect();

    /**
     * Validate whether this channel is connected with AxonServer.
     *
     * @return {@code true} if this channel is connected with AxonServer, {@code false} otherwise
     */
    public abstract boolean isConnected();
}
