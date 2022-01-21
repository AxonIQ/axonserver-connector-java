/*
 * Copyright (c) 2022. AxonIQ
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

package io.axoniq.axonserver.connector;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * {@link Publisher} implementation that allows to interact with a {@link ResultStream}.
 * <p>
 * When subscribed, it generates a new {@link ResultStream} through the provided supplier.
 * The generated {@link ResultStream} provides the elements to be published.
 *
 * @param <M> the type of element consumed from the underlying stream
 * @author Sara Pellegrini
 * @since 4.6
 */
public class ResultStreamPublisher<M> implements Publisher<M> {

    private static final Logger logger = LoggerFactory.getLogger(ResultStreamPublisher.class);
    private final Supplier<ResultStream<M>> resultStreamSupplier;

    public ResultStreamPublisher(Supplier<ResultStream<M>> resultStreamSupplier) {
        this.resultStreamSupplier = resultStreamSupplier;
    }

    @Override
    public void subscribe(Subscriber<? super M> s) {
        ResultStreamSubscription subscription = new ResultStreamSubscription(s, resultStreamSupplier.get());
        s.onSubscribe(subscription);
        subscription.afterSubscribe();
    }

    private class ResultStreamSubscription implements Subscription {

        private final Subscriber<? super M> subscriber;
        private final ResultStream<M> resultStream;
        private final AtomicLong requested = new AtomicLong(0);
        private final AtomicBoolean signalGate = new AtomicBoolean(false);
        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        private final AtomicBoolean subscribed = new AtomicBoolean(false);
        private final AtomicBoolean completed = new AtomicBoolean(false);

        private ResultStreamSubscription(Subscriber<? super M> subscriber,
                                         ResultStream<M> resultStream) {
            this.subscriber = subscriber;
            this.resultStream = resultStream;
            this.resultStream.onAvailable(this::signal);
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                subscriber.onError(new IllegalArgumentException("negative subscription request"));
                return;
            }
            requested.updateAndGet(current -> current + Math.min(Long.MAX_VALUE - current, n));
            signal();
        }

        @Override
        public void cancel() {
            logger.debug("The call has been cancelled.");
            cancelled.set(true);
            resultStream.close();
        }

        private void signal() {
            while (canConsume() && signalGate.compareAndSet(false, true)) {
                try {

                    long requests = requested.get();
                    long counter = 0;
                    for (int i = 0; i < requests; i++) {
                        if (!resultStream.isClosed() && resultStream.peek() != null) {
                            subscriber.onNext(resultStream.next());
                            counter--;
                        } else {
                            break;
                        }
                    }
                    requested.getAndAccumulate(counter, Long::sum);

                    if (resultStream.isClosed()) {
                        subscriber.onComplete();
                        completed.set(true);
                    }
                    resultStream.getError().ifPresent(this::onError);
                } catch (InterruptedException e) {
                    signalGate.set(false);
                    Thread.currentThread().interrupt();
                } catch (Exception ex) {
                    this.onError(ex);
                } finally {
                    signalGate.set(false);
                }
            }
        }

        private void onError(Throwable error) {
            logger.debug("An error occurred accessing the ResultStream.", error);
            subscriber.onError(error);
            completed.set(true);
        }


        private boolean canConsume() {
            try {
                return subscribed.get() &&
                        !cancelled.get() &&
                        !completed.get() &&
                        (resultStream.isClosed() || (resultStream.peek() != null && requested.get() > 0));
            } catch (Exception e) {
                this.onError(e);
                return false;
            }
        }

        private void afterSubscribe() {
            subscribed.set(true);
            signal();
        }
    }
}
