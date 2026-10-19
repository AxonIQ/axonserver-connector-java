/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.event.transformation.event;

import io.axoniq.axonserver.connector.event.transformation.Appender;
import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.function.BiConsumer;

/**
 * Provides a way to transform a given event by invoking operations on the {@link Appender}.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
public interface EventTransformer extends BiConsumer<EventWithToken, Appender> {

}
