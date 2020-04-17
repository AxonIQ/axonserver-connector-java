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

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class AbstractAxonServerChannel {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ScheduledExecutorService executor;

    public AbstractAxonServerChannel(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    protected void scheduleReconnect(ManagedChannel channel) {
        executor.schedule(() -> {
            ConnectivityState connectivityState = channel.getState(true);
            if (connectivityState != ConnectivityState.TRANSIENT_FAILURE
                    && connectivityState != ConnectivityState.SHUTDOWN) {
                connect(channel);
            } else {
                logger.info("Not using this channel to set up stream. It has been disconnected ");
            }
        }, 500, TimeUnit.MILLISECONDS);
    }

    // TODO - Keep reference to latest channel to make connect idempotent when already connected
    public abstract void connect(ManagedChannel channel);

    public abstract void disconnect();

    public abstract boolean isConnected();
}
