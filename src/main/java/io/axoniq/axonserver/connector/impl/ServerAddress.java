/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import java.util.Objects;

/**
 * Definition of an AxonServer address, defining the {@link #getGrpcPort()} and {@link #getHostName()}.
 */
public class ServerAddress {

    /**
     * A default {@link ServerAddress}, using {@link #getHostName() host} {@code "localhost"} and
     * {@link #getGrpcPort() gRPC port} {@code 8124}.
     */
    public static final ServerAddress DEFAULT = new ServerAddress();

    private final int grpcPort;
    private final String host;

    /**
     * Create a {@link ServerAddress} with the default host {@code localhost} and port number {@code 8124}.
     */
    public ServerAddress() {
        this("localhost");
    }

    /**
     * Create a {@link ServerAddress} with the given {@code host} and port number {@code 8124}.
     */
    public ServerAddress(String host) {
        this(host, 8124);
    }

    /**
     * Create a {@link ServerAddress} with the given {@code host} and given {@code port} number.
     */
    public ServerAddress(String host, int grpcPort) {
        this.grpcPort = grpcPort;
        this.host = host;
    }

    /**
     * Return the gRPR port defined in this {@link ServerAddress} instance.
     *
     * @return the gRPR port defined in this {@link ServerAddress} instance
     */
    public int getGrpcPort() {
        return grpcPort;
    }

    /**
     * Return the host name defined in this {@link ServerAddress} instance.
     *
     * @return the host name defined in this {@link ServerAddress} instance
     */
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
