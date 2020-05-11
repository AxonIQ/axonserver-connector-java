package io.axoniq.axonserver.connector;

import java.util.concurrent.TimeUnit;

public interface ResultStream<T> extends AutoCloseable {
    T peek();

    T nextIfAvailable();

    T nextIfAvailable(int timeout, TimeUnit unit) throws InterruptedException;

    T next() throws InterruptedException;

    void onAvailable(Runnable r);

    void close();

    boolean isClosed();
}
