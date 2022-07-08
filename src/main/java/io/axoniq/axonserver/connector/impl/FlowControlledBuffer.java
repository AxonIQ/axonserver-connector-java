/*
 * Copyright (c) 2020. AxonIQ
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

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.grpc.stub.ClientCallStreamObserver;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract implementation of the {@link FlowControlledStream}, adding buffering logic to the flow controlled stream.
 *
 * @param <T> the type of results this implementation buffers
 * @param <R> the type of message used for flow control in this buffer
 */
public abstract class FlowControlledBuffer<T, R> extends FlowControlledStream<T, R> {

    private final BlockingQueue<T> buffer = new LinkedBlockingQueue<>();
    private final AtomicReference<Throwable> errorResult = new AtomicReference<>();

    /**
     * Constructs a {@link FlowControlledBuffer}.
     *
     * @param clientId    the client identifier which initiated this buffer
     * @param bufferSize  the number of entries this buffer should hold
     * @param refillBatch the number of entries to be consumed prior to requesting new once
     */
    public FlowControlledBuffer(String clientId, int bufferSize, int refillBatch) {
        super(clientId, bufferSize, refillBatch);
        if (terminalMessage() == null) {
            throw new IllegalStateException("Terminal message is not allowed to be null");
        }
    }

    /**
     * Builds a terminal message of type {@code T} specifying when the this stream is closed.
     *
     * @return a terminal message of type {@code T} specifying when the this stream is closed
     */
    protected abstract T terminalMessage();

    @Override
    public void onNext(T value) {
        if (value == null) {
            throw new NullPointerException("Next value of buffer is not allowed to be null");
        }
        buffer.offer(value);
    }

    @Override
    public void onError(Throwable t) {
        errorResult.set(t);
        buffer.offer(terminalMessage());
    }

    @Override
    public void onCompleted() {
        buffer.offer(terminalMessage());
    }

    public void close() {
        errorResult.set(new AxonServerException(ErrorCategory.OTHER, "Stream closed on client request", ""));
    }

    /**
     * Returns the error result, if any was recorded. This method may also yield a non-empty result when the buffer
     * still contains messages for processing.
     *
     * @return the error result, if any was recorded, or else {@code null}
     */
    public Throwable getErrorResult() {
        return errorResult.get();
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<R> requestStream) {
        SynchronizedRequestStream<R> synchronizedRequestStream = new SynchronizedRequestStream<>(requestStream);
        super.beforeStart(synchronizedRequestStream);
    }

    /**
     * Try to retrieve an entry of type {@code T} from the buffer immediately. If none is present, {@code null} will be
     * returned.
     *
     * @return an entry of type {@code T} from this buffer if present, otherwise {@code null}
     */
    protected T tryTakeNow() {
        T taken = validate(buffer.poll(), true);

        if (taken != null) {
            markConsumed();
        }
        return taken;
    }

    /**
     * Try to retrieve an entry of type {@code T} from the buffer, waiting for the duration of {@code timeout} in the
     * given {@code timeUnit}. If none is present, {@code null} will be returned.
     *
     * @param timeout  the duration to wait for an entry to become available in the buffer
     * @param timeUnit the {@link TimeUnit} used to specify the duration together with the {@code timeout}
     * @return an entry of type {@code T} from this buffer if present, otherwise {@code null}. Timeouts will result in
     * null as well
     * @throws InterruptedException while waiting for an entry to be taken
     */
    protected T tryTake(long timeout, TimeUnit timeUnit) throws InterruptedException {
        try {
            return tryTake(timeout, timeUnit, false);
        } catch (TimeoutException e) {
            return null;
        }
    }

    /**
     * Try to retrieve an entry of type {@code T} from the buffer, waiting for the duration of {@code timeout} in the
     * given {@code timeUnit}. If none is present, {@code null} will be returned.
     *
     * @param timeout            the duration to wait for an entry to become available in the buffer
     * @param timeUnit           the {@link TimeUnit} used to specify the duration together with the {@code timeout}
     * @param exceptionOnTimeout Whether a {@code TimeoutException} should be thrown when the operation times out
     * @return an entry of type {@code T} from this buffer if present, otherwise {@code null}
     * @throws InterruptedException while waiting for an entry to be taken
     * @throws TimeoutException     If there is no message available after waiting the allotted period
     */
    protected T tryTake(long timeout, TimeUnit timeUnit, boolean exceptionOnTimeout)
            throws InterruptedException, TimeoutException {
        T poll = buffer.poll(timeout, timeUnit);
        if (poll == null && exceptionOnTimeout) {
            throw new TimeoutException("Timeout while trying to peek next event from the stream");
        }
        T taken = validate(poll, true);
        if (taken != null) {
            markConsumed();
        }
        return taken;
    }

    /**
     * Try to retrieve an entry of type {@code T} from the buffer, waiting indefinitely. If none is present because the
     * buffer is closed, {@code null} will be returned.
     *
     * @return an entry of type {@code T} from this buffer if present, otherwise {@code null}
     * @throws InterruptedException while waiting for an entry to be taken
     */
    protected T tryTake() throws InterruptedException {
        T taken = validate(buffer.take(), true);
        if (taken != null) {
            markConsumed();
        }
        return taken;
    }

    /**
     * Take an entry of type {@code T} from the buffer, waiting indefinitely.
     *
     * @return an entry of type {@code T} from this buffer
     * @throws InterruptedException while waiting for an entry to be taken
     */
    protected T take() throws InterruptedException {
        T taken = validate(buffer.take(), false);
        markConsumed();
        return taken;
    }

    /**
     * Retrieves, but does not remove, the first entry of this buffer, or returns {@code null} if the buffer is empty.
     *
     * @return he first entry of this buffer without removing it, or {@code null} if it is empty
     */
    protected T peek() {
        return validate(buffer.peek(), false);
    }

    private T validate(T peek, boolean nullOnTerminal) {
        if (terminalMessage().equals(peek)) {
            if (buffer.isEmpty()) {
                // just to make sure there is always a TERMINAL entry left in a terminated buffer
                buffer.offer(terminalMessage());
            }
            if (nullOnTerminal) {
                return null;
            }
            throw new StreamClosedException(errorResult.get());
        }
        return peek;
    }

    /**
     * Check whether this buffer has been closed off
     *
     * @return {@code true} if this buffer is closed, {@code false otherwise}
     */
    protected boolean isClosed() {
        return terminalMessage().equals(buffer.peek());
    }
}
