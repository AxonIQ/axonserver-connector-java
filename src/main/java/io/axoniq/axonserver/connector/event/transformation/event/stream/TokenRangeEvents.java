/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.event.transformation.event.stream;

import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * {@link EventWithToken}s' {@link Iterable} that start from a given initial token and completes at the last token.
 *
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class TokenRangeEvents implements Iterable<EventWithToken> {

    private final Supplier<EventChannel> eventChannel;

    private final long firstToken;

    private final long lastToken;

    /**
     * Constructs an instance based on the specified parameters.
     *
     * @param eventChannel the channel used to open a stream to Axon Server for reading events
     * @param firstToken   the first token to include
     * @param lastToken    the last token to include
     */
    public TokenRangeEvents(Supplier<EventChannel> eventChannel, long firstToken, long lastToken) {
        this.eventChannel = eventChannel;
        this.firstToken = firstToken;
        this.lastToken = lastToken;
    }

    private static EventWithToken next(EventStream eventStream) {
        try {
            return eventStream.next();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            eventStream.close();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<EventWithToken> iterator() {
        return events(firstToken, lastToken);
    }

    private Iterator<EventWithToken> events(long firstToken, long lastToken) {
        EventStream eventStream = eventChannel.get().openStream(firstToken, 10);
        AtomicReference<EventWithToken> nextRef = new AtomicReference<>(TokenRangeEvents.next(eventStream));
        return new Iterator<EventWithToken>() {
            @Override
            public boolean hasNext() {
                return nextRef.get().getToken() <= lastToken;
            }

            @Override
            public EventWithToken next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (eventStream.isClosed()) {
                    throw new StreamClosedException(eventStream.getError().orElse(null));
                }
                EventWithToken current = nextRef.get();
                if (current.getToken() == lastToken) {
                    nextRef.set(current.toBuilder().setToken(lastToken + 1).build());
                    eventStream.close();
                } else {
                    nextRef.set(TokenRangeEvents.next(eventStream));
                }
                return current;
            }
        };
    }
}
