/*
 * Copyright (c) 2020-2021. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector;

import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests {@link DoOnFirstResultStream}.
 */
class DoOnFirstResultStreamTest {

    private static final String ELEMENT = "element";
    private static final boolean CLOSED = true;

    private ResultStream<String> delegate;
    private Consumer<String> doOnFirstCallback;
    private DoOnFirstResultStream<String> testSubject;

    @BeforeEach
    void setUp() throws InterruptedException {
        //noinspection unchecked
        delegate = mock(ResultStream.class);
        when(delegate.peek()).thenReturn(ELEMENT);
        when(delegate.nextIfAvailable()).thenReturn(ELEMENT);
        when(delegate.nextIfAvailable(anyLong(), any(TimeUnit.class))).thenReturn(ELEMENT);
        when(delegate.next()).thenReturn(ELEMENT);
        when(delegate.isClosed()).thenReturn(CLOSED);
        when(delegate.getError()).thenReturn(Optional.empty());
        //noinspection unchecked
        doOnFirstCallback = mock(Consumer.class);
        testSubject = new DoOnFirstResultStream<>(delegate, doOnFirstCallback);
    }

    @Test
    void testOnFirstCallbackInvokedWhenPeeked() {
        assertEquals(ELEMENT, testSubject.peek());
        verify(doOnFirstCallback).accept(ELEMENT);

        assertEquals(ELEMENT, testSubject.peek());
        verifyNoMoreInteractions(doOnFirstCallback);
        verify(delegate, times(2)).peek();
    }

    @Test
    void testOnFirstCallbackInvokedWhenNextIfAvailable() {
        assertEquals(ELEMENT, testSubject.nextIfAvailable());
        verify(doOnFirstCallback).accept(ELEMENT);

        assertEquals(ELEMENT, testSubject.nextIfAvailable());
        verifyNoMoreInteractions(doOnFirstCallback);
        verify(delegate, times(2)).nextIfAvailable();
    }

    @Test
    void testOnFirstCallbackInvokedWhenNextIfAvailableWithTimeout() throws InterruptedException {
        assertEquals(ELEMENT, testSubject.nextIfAvailable(1, TimeUnit.SECONDS));
        verify(doOnFirstCallback).accept(ELEMENT);

        assertEquals(ELEMENT, testSubject.nextIfAvailable(1, TimeUnit.SECONDS));
        verifyNoMoreInteractions(doOnFirstCallback);
        verify(delegate, times(2)).nextIfAvailable(1, TimeUnit.SECONDS);
    }

    @Test
    void testOnFirstCallbackInvokedWhenNext() throws InterruptedException {
        assertEquals(ELEMENT, testSubject.next());
        verify(doOnFirstCallback).accept(ELEMENT);

        assertEquals(ELEMENT, testSubject.next());
        verifyNoMoreInteractions(doOnFirstCallback);
        verify(delegate, times(2)).next();
    }

    @Test
    void testDelegationOfNonConsumableMethods() {
        assertTrue(testSubject.isClosed());
        assertEquals(Optional.empty(), testSubject.getError());
        testSubject.close();
        testSubject.onAvailable(null);

        verify(delegate).isClosed();
        verify(delegate).getError();
        verify(delegate).close();
        verify(delegate).onAvailable(null);
    }

    @RepeatedTest(1000)
    void testOnFirstIsCalledOnlyOnce() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        List<Runnable> actions = asList(() -> testSubject.peek(),
                                        this::silentNext,
                                        () -> testSubject.nextIfAvailable(),
                                        this::silentNextIfAvailable);
        Collections.shuffle(actions);

        actions.stream()
               .map(executorService::submit)
               .forEach(this::completeFuture);

        verify(doOnFirstCallback).accept(ELEMENT);
        verifyNoMoreInteractions(doOnFirstCallback);
        verify(delegate).peek();
        verify(delegate).next();
        verify(delegate).nextIfAvailable(1, TimeUnit.SECONDS);
        verify(delegate).nextIfAvailable();
    }

    private void silentNext() {
        try {
            testSubject.next();
        } catch (InterruptedException e) {
            fail("next() threw an exception", e);
        }
    }

    private void silentNextIfAvailable() {
        try {
            testSubject.nextIfAvailable(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("nextIfAvailable(long, TimeUnit) threw an exception", e);
        }
    }

    private void completeFuture(Future<?> future) {
        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail("future didn't complete", e);
        }
    }
}
