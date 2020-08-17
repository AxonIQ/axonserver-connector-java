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

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * Several general utilities to deal with objects and the like.
 */
public abstract class ObjectUtils {

    private ObjectUtils() {
        // utility class
    }

    /**
     * Returns the given {@code instance} if it is not {@code null}. Otherwise {@code ifNull} is returned.
     *
     * @param instance the object to validate if it is no {@code null}
     * @param ifNull   the object to return if {@code instance} is {@code null}
     * @param <T>      the type of the given {@code instance} and {@code ifNull}
     * @return the given {@code instance} if it is not {@code null}. Otherwise {@code ifNull} is returned
     */
    public static <T> T nonNullOrDefault(T instance, T ifNull) {
        if (instance == null) {
            return ifNull;
        }
        return instance;
    }

    /**
     * Performs the given {@code action} consuming the given {@code instance}, granted that it is not {@code null}.
     *
     * @param instance the object to consume by the given {@code action}, <em>if</em> it is not {@code null}
     * @param action   the {@link Consumer} invoked with the given {@code instance}
     * @param <T>      the type of the given {@code instance} and object to consume by the given {@code action}
     * @return an {@link OrElse} to allow to perform another operation if the given {@code action} was not executed
     */
    public static <T> OrElse doIfNotNull(T instance, Consumer<T> action) {
        if (instance != null) {
            action.accept(instance);
            return OrElse.ignore();
        }
        return OrElse.execute();
    }

    /**
     * Silently performs the given {@code action} consuming the given {@code instance}, granted that it is not {@code
     * null}. Will swallow any exceptions thrown by the {@code action}
     *
     * @param instance the object to consume by the given {@code action}, <em>if</em> it is not {@code null}
     * @param action   the {@link Consumer} to silently invoke with the given {@code instance}
     * @param <T>the   type of the given {@code instance} and object to consume by the given {@code action}
     */
    public static <T> void silently(T instance, Consumer<T> action) {
        if (instance != null) {
            try {
                action.accept(instance);
            } catch (Exception e) {
                // Silent, remember
            }
        }
    }

    /**
     * Verify if the given {@link String} is not {@code null} and is not {@link String#isEmpty()}.
     *
     * @param s the {@link String} to validate if it has any length.
     * @return {@code true} if the given {@link String} is not {@code null} or empty, {@code false} otherwise
     */
    public static boolean hasLength(String s) {
        return s != null && !s.isEmpty();
    }

    /**
     * Creates a random {@link String} of the given {@code length}.
     *
     * @param length the size of the random {@link String} to return
     * @return a random {@link String} of the given {@code length}
     */
    public static String randomHex(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(Integer.toString(ThreadLocalRandom.current().nextInt(0, 16), 16));
        }
        return sb.toString();
    }

    /**
     * Class allowing the invocation of a given {@link Runnable} depending on whether the previous operation was
     * executed, formulated as a {@code boolean}.
     */
    public static class OrElse {

        private static final OrElse IGNORE = new OrElse(false);
        private static final OrElse EXECUTE = new OrElse(true);

        private final boolean executeRunnable;

        /**
         * Constructs an {@link OrElse} with the given {@code executeRunnable} defining if the {@link #orElse(Runnable)}
         * {@link Runnable} should be executed.
         *
         * @param executeRunnable a {@code boolean} defining if the {@link #orElse(Runnable)} {@link Runnable} should be
         *                        executed.
         */
        public OrElse(boolean executeRunnable) {
            this.executeRunnable = executeRunnable;
        }

        private static OrElse ignore() {
            return IGNORE;
        }

        private static OrElse execute() {
            return EXECUTE;
        }

        /**
         * Runs the given {@code runnable} depending on whether the previous executable was invoked, formulated through
         * the {@code executeRunnable} set in the constructor.
         *
         * @param runnable the {@link Runnable} to execute if {@code executeRunnable} was set to {@code true}
         */
        public void orElse(Runnable runnable) {
            if (executeRunnable) {
                runnable.run();
            }
        }
    }
}
