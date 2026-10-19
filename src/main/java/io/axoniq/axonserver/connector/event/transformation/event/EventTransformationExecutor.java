/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.event.transformation.event;

import io.axoniq.axonserver.connector.event.transformation.EventTransformation;
import io.axoniq.axonserver.connector.event.transformation.EventTransformationChannel;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Executes the Transformation by invoking APPLY operation.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public interface EventTransformationExecutor {

    /**
     * Executes the Transformation by invoking APPLY operation.
     *
     * @param channelSupplier supplies the {@link EventTransformationChannel}
     * @return a {@link CompletableFuture} to indicate when the APPLY process has been accepted
     */
    CompletableFuture<EventTransformation> execute(Supplier<EventTransformationChannel> channelSupplier);
}
