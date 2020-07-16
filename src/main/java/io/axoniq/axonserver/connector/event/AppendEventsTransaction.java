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

package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;

import java.util.concurrent.CompletableFuture;

/**
 * Interface providing operations to interact with a Transaction to append events onto the Event Store.
 */
public interface AppendEventsTransaction {

    /**
     * Append the given event to be committed as part of this transaction.
     *
     * @param event the event to append
     * @return this instance for fluent interfacing
     */
    AppendEventsTransaction appendEvent(Event event);

    /**
     * Commit this transaction, appending all registered events into the Event Store.
     *
     * @return a CompletableFuture resolving the confirmation of the successful processing of the transaction
     */
    CompletableFuture<Confirmation> commit();

    /**
     * Rolls back the transaction.
     */
    void rollback();
}
