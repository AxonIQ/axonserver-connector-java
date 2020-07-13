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

import java.util.Objects;

public class ServerAddress {

    public static final ServerAddress DEFAULT = new ServerAddress();

    private final int grpcPort;
    private final String host;

    public ServerAddress() {
        this("localhost");
    }

    public ServerAddress(String host) {
        this(host, 8124);
    }

    public ServerAddress(String host, int grpcPort) {
        this.grpcPort = grpcPort;
        this.host = host;
    }

    public int getGrpcPort() {
        return grpcPort;
    }

    public String getHostName() {
        return host;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServerAddress that = (ServerAddress) o;
        return grpcPort == that.grpcPort &&
                host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(grpcPort, host);
    }

    @Override
    public String toString() {
        return host + ":" + grpcPort;
    }
}
