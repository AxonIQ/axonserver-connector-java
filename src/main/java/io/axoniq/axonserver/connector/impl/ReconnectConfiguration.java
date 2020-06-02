package io.axoniq.axonserver.connector.impl;

import java.util.concurrent.TimeUnit;

public class ReconnectConfiguration {

    private final long connectTimeout;
    private final long reconnectInterval;
    private final boolean forcePlatformReconnect;
    private final TimeUnit timeUnit;

    public ReconnectConfiguration(long connectTimeout, long reconnectInterval, boolean forcePlatformReconnect, TimeUnit timeUnit) {
        this.connectTimeout = connectTimeout;
        this.reconnectInterval = reconnectInterval;
        this.forcePlatformReconnect = forcePlatformReconnect;
        this.timeUnit = timeUnit;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public long getReconnectInterval() {
        return reconnectInterval;
    }

    public boolean isForcePlatformReconnect() {
        return forcePlatformReconnect;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

}
