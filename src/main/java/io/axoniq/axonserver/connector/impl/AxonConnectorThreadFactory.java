/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link ThreadFactory} implementation for AxonServer Connector threads.
 */
public class AxonConnectorThreadFactory implements ThreadFactory {

    private final ThreadGroup threadGroup;
    private final AtomicInteger threadNumber = new AtomicInteger();

    private AxonConnectorThreadFactory(String instanceId) {
        threadGroup = new ThreadGroup("axon-connector-" + instanceId);
    }

    /**
     * Constructs an {@link AxonConnectorThreadFactory} using the given {@code instanceId} as part of the thread group.
     *
     * @param instanceId the instance identifier to become part of the thread group
     * @return an {@link AxonConnectorThreadFactory} used to create threads
     */
    public static AxonConnectorThreadFactory forInstanceId(String instanceId) {
        return new AxonConnectorThreadFactory(instanceId);
    }

    @Override
    @SuppressWarnings("NullableProblems")
    public Thread newThread(Runnable runnable) {
        return new Thread(threadGroup, runnable, threadGroup.getName() + "-" + nextThreadNumber());
    }

    private int nextThreadNumber() {
        return threadNumber.getAndIncrement();
    }
}