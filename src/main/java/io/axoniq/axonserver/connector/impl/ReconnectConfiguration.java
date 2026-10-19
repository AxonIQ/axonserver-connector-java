/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import java.util.concurrent.TimeUnit;

/**
 * Container of configuration variables used when reconnecting to a channel.
 */
public class ReconnectConfiguration {

    private final long connectTimeout;
    private final long reconnectInterval;
    private final boolean forcePlatformReconnect;
    private final TimeUnit timeUnit;

    /**
     * Construct a new {@link ReconnectConfiguration}.
     *
     * @param connectTimeout         a {@code long} defining the connect timeout
     * @param reconnectInterval      a {@code long} defining the interval when to reconnect
     * @param forcePlatformReconnect a {@code boolean} defining whether a platform reconnect should be enforced
     * @param timeUnit               the {@link TimeUnit} used for both the {@code connectTimeout} and {@code
     *                               reconnectInterval}
     */
    public ReconnectConfiguration(long connectTimeout,
                                  long reconnectInterval,
                                  boolean forcePlatformReconnect,
                                  TimeUnit timeUnit) {
        this.connectTimeout = connectTimeout;
        this.reconnectInterval = reconnectInterval;
        this.forcePlatformReconnect = forcePlatformReconnect;
        this.timeUnit = timeUnit;
    }

    /**
     * Return the configured connect timeout.
     *
     * @return the configured connect timeout
     */
    public long getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Return the configured reconnect timeout.
     *
     * @return the configured reconnect timeout
     */
    public long getReconnectInterval() {
        return reconnectInterval;
    }

    /**
     * Return whether a platform reconnect should be enforced.
     *
     * @return whether a platform reconnect should be enforced
     */
    public boolean isForcePlatformReconnect() {
        return forcePlatformReconnect;
    }

    /**
     * Return the configured {@link TimeUnit} used by {@link #getConnectTimeout()} and {@link #getReconnectInterval()}.
     *
     * @return the configured {@link TimeUnit} used by {@link #getConnectTimeout()} and {@link #getReconnectInterval()}
     */
    public TimeUnit getTimeUnit() {
        return timeUnit;
    }
}
