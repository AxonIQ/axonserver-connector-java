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

package io.axoniq.axonserver.connector.testutils;

import org.junit.jupiter.api.Assertions;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Utility class for special assertions
 */
public abstract class AssertUtils {

    private AssertUtils() {
        // Utility class
    }

    /**
     * Assert that the given {@code assertion} succeeds with the given {@code time} and {@code unit}.
     *
     * @param time      an {@code int} which paired with the {@code unit} specifies the time in which the assertion must
     *                  pass
     * @param unit      a {@link TimeUnit} in which {@code time} is expressed
     * @param assertion a {@link Runnable} containing the assertion to succeed within the deadline
     */
    @SuppressWarnings("Duplicates")
    public static void assertWithin(int time, TimeUnit unit, ExceptionThrowingRunnable assertion) {
        long now = System.currentTimeMillis();
        long deadline = now + unit.toMillis(time);
        do {
            try {
                assertion.run();
                break;
            } catch (AssertionError e) {
                if (now >= deadline) {
                    throw e;
                }
            } catch (Exception e) {
                if (now >= deadline) {
                    Assertions.fail(e);
                }
            }
            now = System.currentTimeMillis();
        } while (true);
    }

    /**
     * Assert that the given {@code assertion} returns {@code true} within the given {@code time} and {@code unit}.
     *
     * @param time            an {@code int} which paired with the {@code unit} specifies the time in which the
     *                        assertion must
     *                        pass
     * @param unit            a {@link TimeUnit} in which {@code time} is expressed
     * @param booleanSupplier a {@link BooleanSupplier} returning th value to assert
     */
    @SuppressWarnings("Duplicates")
    public static void assertTrueWithin(int time, TimeUnit unit, BooleanSupplier booleanSupplier) {
        assertWithin(time, unit, () -> assertTrue(booleanSupplier));
    }

    /**
     * Assert that the given {@code assertion} returns {@code false} within the given {@code time} and {@code unit}.
     *
     * @param time            an {@code int} which paired with the {@code unit} specifies the time in which the
     *                        assertion must
     *                        pass
     * @param unit            a {@link TimeUnit} in which {@code time} is expressed
     * @param booleanSupplier a {@link BooleanSupplier} returning th value to assert
     */
    @SuppressWarnings("Duplicates")
    public static void assertFalseWithin(int time, TimeUnit unit, BooleanSupplier booleanSupplier) {
        assertWithin(time, unit, () -> assertFalse(booleanSupplier));
    }

    @FunctionalInterface
    public interface ExceptionThrowingRunnable {

        void run() throws Exception;

    }
}