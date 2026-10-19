/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.event.transformation.impl.grpc;

import io.axoniq.axonserver.connector.event.transformation.EventTransformation;
import io.axoniq.axonserver.grpc.event.Transformation;

/**
 * Implementation of {@link EventTransformation} that wraps a Proto message representing the transformation, and uses it
 * to provide the requested details.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public class GrpcEventTransformation implements EventTransformation {

    private final Transformation transformation;

    /**
     * Constructs an instance based on the proto message representing the transformation.
     *
     * @param transformation the proto message containing the details of the transformation.
     */
    GrpcEventTransformation(Transformation transformation) {
        this.transformation = transformation;
    }

    @Override
    public String id() {
        return transformation.getTransformationId().getId();
    }

    @Override
    public String description() {
        return transformation.getDescription();
    }

    @Override
    public Long lastSequence() {
        return transformation.getSequence();
    }

    @Override
    public State state() {
        return State.valueOf(transformation.getState().name());
    }

    @Override
    public String toString() {
        return "GrpcEventTransformation{" +
                "transformation=" + transformation +
                '}';
    }
}
