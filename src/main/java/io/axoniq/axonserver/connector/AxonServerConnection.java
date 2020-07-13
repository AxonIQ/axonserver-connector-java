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

package io.axoniq.axonserver.connector;

import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.connector.control.ControlChannel;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.query.QueryChannel;

/**
 * Interface exposing available operations on a connection to AxonServer. In multi-context connections, each connection
 * to a context will be represented by its own instance.
 */
public interface AxonServerConnection {

    /**
     * Indicates whether the underlying connection failed. This may occur when AxonServer cannot be reached.
     *
     * @return {@code true} when the underlying connection failed, otherwise {@code false}
     */
    boolean isConnectionFailed();

    /**
     * Indicates whether the connection is ready to process and receive instructions. A connection is considered
     * <em>ready</em> when it is connected (see {@link #isConnected()} and if all previously active communication
     * channels (e.g. command, query, instruction) have been activated.
     *
     * @return {@code true} if the underlying connection is ready, otherwise {@code false}
     */
    boolean isReady();

    /**
     * Indicates whether the underlying connection is active. This means a network connection has been made with an
     * AxonServer instance.
     *
     * @return {@code true} if a network connection to AxonServer is available, otherwise {@code false}
     */
    boolean isConnected();

    /**
     * Disconnects all communication channels and terminates any active network connections to AxonServer. No
     * more operations can be performed on this connection after invoking <em>disconnect()</em>.
     */
    void disconnect();

    /**
     * Returns the channel on which platform control messages can be sent and received
     *
     * @return the channel for platform instruction
     */
    ControlChannel controlChannel();

    /**
     * Returns the channel on which Command related interactions can be performed with AxonServer.
     *
     * @return the channel for Command messaging
     */
    CommandChannel commandChannel();

    /**
     * Returns the channel on which Event related interactions can be performed with AxonServer.
     *
     * @return the channel for Event messaging
     */
    EventChannel eventChannel();

    /**
     * Returns the channel on which Query related interactions can be performed with AxonServer.
     *
     * @return the channel for Query messaging
     */
    QueryChannel queryChannel();
}
