/*
 * Copyright (c) 2020-2023. AxonIQ
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

package io.axoniq.axonserver.connector.event.transformation.impl;

import io.axoniq.axonserver.connector.event.transformation.ActiveTransformation;
import io.axoniq.axonserver.connector.event.transformation.EventTransformation;
import io.axoniq.axonserver.connector.event.transformation.EventTransformationChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.grpc.control.ClientIdentification;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.StreamSupport;

import static io.axoniq.axonserver.connector.event.transformation.EventTransformation.State.ACTIVE;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class EventTransformationChannelImpl
        extends AbstractAxonServerChannel<Void>
        implements EventTransformationChannel {

    private static final Long INITIAL_SEQUENCE = -1L;
    private final EventTransformationService service;

    public EventTransformationChannelImpl(ClientIdentification clientIdentification,
                                          ScheduledExecutorService executor,
                                          AxonServerManagedChannel axonServerManagedChannel) {
        this(clientIdentification,
             executor,
             axonServerManagedChannel,
             new GrpcEventTransformationService(axonServerManagedChannel));
    }

    public EventTransformationChannelImpl(ClientIdentification clientIdentification,
                                          ScheduledExecutorService executor,
                                          AxonServerManagedChannel axonServerManagedChannel,
                                          EventTransformationService service) {
        super(clientIdentification, executor, axonServerManagedChannel);
        this.service = service;
    }

    @Override
    public CompletableFuture<Iterable<EventTransformation>> transformations() {
        return service.transformations();
    }

    @Override
    public CompletableFuture<ActiveTransformation> activeTransformation() {
        return service.transformations()
                      .thenApply(iterable -> StreamSupport
                              .stream(iterable.spliterator(), false)
                              .filter(t -> ACTIVE.equals(t.state()))
                              .findFirst()
                              .map(this::activeTransformation)
                              .orElseThrow(IllegalStateException::new));
    }

    private ActiveTransformation activeTransformation(EventTransformation eventTransformation) {
        return newActiveTransformation(eventTransformation.id(), eventTransformation.lastSequence());
    }

    @Override
    public CompletableFuture<ActiveTransformation> newTransformation(String description) {
        return service.newTransformation(description)
                      .thenApply(id -> newActiveTransformation(id, INITIAL_SEQUENCE));
    }

    private ActiveTransformation newActiveTransformation(String id, long currentSequence) {
        return new ServiceActiveTransformation(id, currentSequence, service);
    }


    @Override
    public CompletableFuture<Void> startCompacting() {
        return service.startCompacting();
    }

    @Override
    public void connect() {
        //do nothing
    }

    @Override
    public void reconnect() {
        //do nothing
    }

    @Override
    public void disconnect() {
        //do nothing
    }

    @Override
    public boolean isReady() {
        return true;
    }
}
