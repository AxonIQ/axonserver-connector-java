/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector;

/**
 * Interface describing a component that is capable of handling instructions coming from AxonServer.
 *
 * @param <I> the type of the instruction handled by this handler
 * @param <R> the type of response used by {@link #handle(Object, ReplyChannel)}'s {@link ReplyChannel}
 */
@FunctionalInterface
public interface InstructionHandler<I, R> {

    /**
     * Handle the given {@code instruction}, using {@code replyChannel} to acknowledge the instruction and reply to
     * AxonServer.
     *
     * @param instruction  the instruction to handle
     * @param replyChannel the channel onto which replies and acknowledgements to send
     */
    void handle(I instruction, ReplyChannel<R> replyChannel);
}
