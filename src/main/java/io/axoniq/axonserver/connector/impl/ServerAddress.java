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
