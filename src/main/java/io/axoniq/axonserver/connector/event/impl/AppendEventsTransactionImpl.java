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

package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.event.AppendEventsTransaction;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;

public class AppendEventsTransactionImpl implements AppendEventsTransaction {

    private final StreamObserver<Event> stream;
    private final CompletableFuture<Confirmation> result;

    public AppendEventsTransactionImpl(StreamObserver<Event> stream, CompletableFuture<Confirmation> result) {
        this.stream = stream;
        this.result = result;
    }

    @Override
    public AppendEventsTransaction appendEvent(Event event) {
        stream.onNext(event);
        return this;
    }

    @Override
    public CompletableFuture<Confirmation> commit() {
        stream.onCompleted();
        return result;
    }

    @Override
    public void rollback() {
        stream.onError(new StatusRuntimeException(Status.CANCELLED));
    }
}
