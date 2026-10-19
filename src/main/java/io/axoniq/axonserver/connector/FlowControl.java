/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector;

/**
 * Controls the flow of the messages via communication channel. Implementations should send messages only when
 * requested. Once the cancellation is invoked, implementations should stop sending messages.
 *
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Allard Buijze
 * @since 4.6.0
 */
public interface FlowControl {

    /**
     * Requests the {@code requested} amount of responses to be sent.
     *
     * @param requested number of responses to be sent
     */
    void request(long requested);

    /**
     * Cancels response sending.
     */
    void cancel();
}
