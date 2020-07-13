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
