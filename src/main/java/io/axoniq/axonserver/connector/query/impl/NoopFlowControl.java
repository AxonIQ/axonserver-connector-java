/*
 * Copyright (c) 2020-2021. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
