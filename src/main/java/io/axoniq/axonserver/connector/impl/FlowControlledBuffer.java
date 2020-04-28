package io.axoniq.axonserver.connector.impl;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class FlowControlledBuffer<T, R> extends FlowControlledStream<T, R> {

    private final T terminalMessage;
    private final BlockingQueue<T> buffer;
    private final AtomicReference<Throwable> errorResult = new AtomicReference<>();

    public FlowControlledBuffer(String clientId, T terminalMessage, int bufferSize, int refillBatch) {
        super(clientId, bufferSize, refillBatch);
        this.terminalMessage = terminalMessage;
        this.buffer = (bufferSize > Integer.MAX_VALUE >> 1) ? new LinkedBlockingQueue<>() : new ArrayBlockingQueue<>(bufferSize + 1);
    }

    @Override
    public void onNext(T value) {
        buffer.offer(value);
    }

    @Override
    public void onError(Throwable t) {
        errorResult.set(t);
        buffer.offer(terminalMessage);
    }

    @Override
    public void onCompleted() {
        buffer.offer(terminalMessage);
    }

    protected T poll(int timeout, TimeUnit unit) throws InterruptedException {
        T taken = validate(buffer.poll(timeout, unit));
        if (taken != null) {
            markConsumed();
        }
        return taken;
    }

    protected T poll() {
        T taken = validate(buffer.poll());
        if (taken != null) {
            markConsumed();
        }
        return taken;
    }

    protected T tryTakeNow() {
        T taken = validate(buffer.poll(), true);

        markConsumed();
        return taken;
    }

    protected T tryTake() throws InterruptedException {
        T taken = validate(buffer.take(), true);
        markConsumed();
        return taken;
    }

    protected T take() throws InterruptedException {
        T taken = validate(buffer.take());
        markConsumed();
        return taken;
    }

    protected T peek() {
        return validate(buffer.peek(), true);
    }

    private T validate(T peek) {
        return validate(peek, false);
    }

    private T validate(T peek, boolean nullOnTerminal) {
        if (terminalMessage.equals(peek)) {
            if (buffer.isEmpty()) {
                // just to make sure there is always a TERMINAL entry left in a terminated buffer
                buffer.offer(terminalMessage);
            }
            if (nullOnTerminal) {
                return null;
            }
            throw new StreamClosedException(errorResult.get());
        }
        return peek;
    }

    protected boolean isClosed() {
        return terminalMessage.equals(buffer.peek());
    }
}
