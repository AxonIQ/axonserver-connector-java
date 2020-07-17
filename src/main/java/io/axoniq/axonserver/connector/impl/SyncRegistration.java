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

import io.axoniq.axonserver.connector.Registration;

import java.util.concurrent.CompletableFuture;

/**
 * Synchronous implementation of the {@link Registration}.
 */
public class SyncRegistration implements Registration {

    private static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);
    private final Runnable cancelAction;

    /**
     * Construct a {@link SyncRegistration}, using the given {@code cancelAction} when {@link #cancel()} is invoked.
     *
     * @param cancelAction the operation to perform when {@link #cancel()} is invoked
     */
    public SyncRegistration(Runnable cancelAction) {
        this.cancelAction = cancelAction;
    }

    @Override
    public CompletableFuture<Void> cancel() {
        cancelAction.run();
        return COMPLETED;
    }
}
