package io.axoniq.axonserver.connector;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class AxonConnectorThreadFactory implements ThreadFactory {

    private final ThreadGroup threadGroup;
    private final AtomicInteger threadNumber = new AtomicInteger();

    private AxonConnectorThreadFactory(String instanceId) {
        threadGroup = new ThreadGroup("axon-connector-" + instanceId);
    }

    public static AxonConnectorThreadFactory forInstanceId(String instanceId) {
        return new AxonConnectorThreadFactory(instanceId);
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(threadGroup, r, threadGroup.getName() + "-" + nextThreadNumber());
    }

    private int nextThreadNumber() {
        return threadNumber.getAndIncrement();
    }
}