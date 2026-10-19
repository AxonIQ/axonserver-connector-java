/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link ResultStreamPublisher}.
 *
 * @author Sara Pellegrini
 */
public class ResultStreamPublisherTest extends PublisherVerification<Long> {

    private final TestEnvironment env;

    public ResultStreamPublisherTest() {
        this(new TestEnvironment(1_000));
    }

    private ResultStreamPublisherTest(TestEnvironment env) {
        super(env);
        this.env = env;
    }

    @Override
    public Publisher<Long> createPublisher(long elements) {
        return new ResultStreamPublisher<>(() -> new StubResultStream(elements));
    }

    @Override
    public Publisher<Long> createFailedPublisher() {
        return new ResultStreamPublisher<>(FailedResultStream::new);
    }

    /**
     * Tests the rule 17 of Reactive Stream Specification for Subscription.
     *
     * @see <a href="https://github.com/reactive-streams/reactive-streams-jvm/tree/v1.0.3#3-subscription-code">
     * Reactive Stream Specification - Subscription - rule 17</a>
     */
    @Test
    public void required_spec317_mustSupportACumulativePendingElementCountGreaterThenLongMaxValue() throws Throwable {
        final int totalElements = 5;

        activePublisherTest(totalElements, true, pub -> {
            final TestEnvironment.ManualSubscriber<Long> sub = env.newManualSubscriber(pub);
            new Thread(() -> sub.request(Long.MAX_VALUE)).start();
            new Thread(() -> sub.request(Long.MAX_VALUE)).start();

            sub.nextElements(totalElements);
            sub.expectCompletion();

            try {
                env.verifyNoAsyncErrorsNoDelay();
            } finally {
                sub.cancel();
            }
        });
    }
}