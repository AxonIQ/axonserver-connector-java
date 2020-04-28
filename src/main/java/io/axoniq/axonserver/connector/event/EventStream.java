package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.concurrent.TimeUnit;

public interface EventStream {

    EventWithToken peek();

    EventWithToken nextIfAvailable();

    EventWithToken nextIfAvailable(int timeout, TimeUnit unit) throws InterruptedException;

    EventWithToken next() throws InterruptedException;

    void onAvailable(Runnable r);

    void close();

    boolean isClosed();
}
