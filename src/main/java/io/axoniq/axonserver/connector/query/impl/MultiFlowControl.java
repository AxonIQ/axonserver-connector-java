package io.axoniq.axonserver.connector.query.impl;

import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.query.QueryHandler;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An implementation of {@link QueryHandler.FlowControl} that delegates operations to several registered {@link
 * QueryHandler.FlowControl} implementations.
 */
public class MultiFlowControl implements QueryHandler.FlowControl {

    private final List<QueryHandler.FlowControl> delegates = new CopyOnWriteArrayList<>();

    /**
     * Adds given {@code flowControl} to the list of delegates to be invoked once the {@link QueryHandler.FlowControl}
     * operations is triggered.
     *
     * @param flowControl the {@link QueryHandler.FlowControl} implementation to be registered as a delegate
     * @return a registration which can be used to remove this {@link QueryHandler.FlowControl} from the list of
     * delegates
     */
    public Registration add(QueryHandler.FlowControl flowControl) {
        delegates.add(flowControl);
        return () -> CompletableFuture.runAsync(() -> delegates.remove(flowControl));
    }

    @Override
    public void request(long requested) {
        delegates.parallelStream()
                 .forEach(flowControl -> flowControl.request(requested));
    }

    @Override
    public void complete() {
        delegates.parallelStream()
                 .forEach(QueryHandler.FlowControl::complete);
    }
}
