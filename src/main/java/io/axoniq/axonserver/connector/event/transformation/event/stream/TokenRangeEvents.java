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
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class TokenRangeEvents implements Iterable<EventWithToken> {

    private final Supplier<EventChannel> eventChannel;

    private final long firstToken;

    private final long lastToken;

    public TokenRangeEvents(Supplier<EventChannel> eventChannel, long firstToken, long lastToken) {
        this.eventChannel = eventChannel;
        this.firstToken = firstToken;
        this.lastToken = lastToken;
    }

    private static EventWithToken next(EventStream eventStream) {
        try {
//            return eventStream.nextIfAvailable();
            return eventStream.next();
            //  } catch (InterruptedException e) {
        } catch (Exception e) {
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
        AtomicReference<EventWithToken> prev = new AtomicReference<>(TokenRangeEvents.next(eventStream));
        return new Iterator<EventWithToken>() {
            @Override
            public boolean hasNext() {
                return prev.get().getToken() < lastToken;
            }

            @Override
            public EventWithToken next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (eventStream.isClosed()) {
                    throw new StreamClosedException(eventStream.getError().orElse(null));
                }
                EventWithToken next = TokenRangeEvents.next(eventStream);
                if (next.getToken() == lastToken) {
                    eventStream.close();
                }
                prev.set(next);
                return next;
            }
        };
    }
}
