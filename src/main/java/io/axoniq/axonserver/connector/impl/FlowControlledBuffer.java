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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class FlowControlledBuffer<T, R> extends FlowControlledStream<T, R> {

    private final BlockingQueue<T> buffer = new LinkedBlockingQueue<>();
    private final AtomicReference<Throwable> errorResult = new AtomicReference<>();

    public FlowControlledBuffer(String clientId, int bufferSize, int refillBatch) {
        super(clientId, bufferSize, refillBatch);
    }

    protected abstract T terminalMessage();

    @Override
    public void onNext(T value) {
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

    protected T tryTakeNow() {
        T taken = validate(buffer.poll(), true);

        if (taken != null) {
            markConsumed();
        }
        return taken;
    }

    protected T tryTake(long timeout, TimeUnit timeUnit) throws InterruptedException {
        T taken = validate(buffer.poll(timeout, timeUnit), true);
        if (taken != null) {
            markConsumed();
        }
        return taken;
    }

    protected T tryTake() throws InterruptedException {
        T taken = validate(buffer.take(), true);
        if (taken != null) {
            markConsumed();
        }
        return taken;
    }

    protected T take() throws InterruptedException {
        T taken = validate(buffer.take(), false);
        markConsumed();
        return taken;
    }

    protected T peek() {
        return validate(buffer.peek(), true);
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

    protected boolean isClosed() {
        return terminalMessage().equals(buffer.peek());
    }
}
