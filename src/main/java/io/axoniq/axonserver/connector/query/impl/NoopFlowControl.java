package io.axoniq.axonserver.connector.query.impl;

import io.axoniq.axonserver.connector.query.QueryHandler;

/**
 * NOOP implementation of {@link QueryHandler.FlowControl}.
 */
public enum NoopFlowControl implements QueryHandler.FlowControl {

    /**
     * Singleton instance of {@link NoopFlowControl}.
     */
    INSTANCE;

    @Override
    public void request(long requested) {
        // noop
    }

    @Override
    public void cancel() {
        // noop
    }
}
