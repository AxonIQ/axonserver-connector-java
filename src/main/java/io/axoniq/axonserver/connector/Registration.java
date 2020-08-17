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

package io.axoniq.axonserver.connector;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Interface describing an instruction to perform a registration, which can be cancelled.
 */
@FunctionalInterface
public interface Registration {

    /**
     * Cancel the registration from which this instance was returned. Does nothing if the registration has already been
     * cancelled, or when the registration was undone by another mechanism (such as a new registration overriding this
     * one).
     *
     * @return a {@link CompletableFuture} of {@link Void} to react when {@code this} {@link Registration} has been
     * canceled
     */
    CompletableFuture<Void> cancel();

    /**
     * Wait for the acknowledgement of the original instruction {@code this} {@link Registration} corresponds to. Allows
     * for the addition of further logic to {@code this Registration}, like invoking {@link #onAck(Runnable)} for
     * example.
     *
     * @param timeout the duration to wait until the operation has been acknowledged
     * @param unit    the {@link TimeUnit} for the given {@code timeout} to wait until the operation has been
     *                acknowledged
     * @return {@code this} {@link Registration} to support a fluent API
     * @throws TimeoutException     is thrown when the given {@code timeout} and {@code unit} is surpassed
     * @throws InterruptedException is thrown when the thread waiting for the acknowledgement is interrupted
     */
    default Registration awaitAck(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
        return this;
    }

    /**
     * Registers the given {@code runnable} to {@code this} {@link Registration} to be executed when the acknowledgement
     * of {@code this} {@link Registration} is received. Allows for the addition of further logic to {@code this
     * Registration}, like invoking {@link #awaitAck(long, TimeUnit)} for example.
     *
     * @param runnable the {@link Runnable} to execute when the acknowledgement of {@code this} {@link Registration} is
     *                 received
     * @return {@code this} {@link Registration} to support a fluent API
     */
    default Registration onAck(Runnable runnable) {
        runnable.run();
        return this;
    }
}
