/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.FlowControl;

/**
 * NOOP implementation of {@link FlowControl}.
 *
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 * @since 4.6.0
 */
public enum NoopFlowControl implements FlowControl {

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
