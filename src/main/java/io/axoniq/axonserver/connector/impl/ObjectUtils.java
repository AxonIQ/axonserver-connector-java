/*
 * Copyright (c) 2010-2020. Axon Framework
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

public class ObjectUtils {

    private ObjectUtils() {
        // utility class
    }

    public static <T> T nonNullOrDefault(T instance, T ifNull) {
        if (instance == null) {
            return ifNull;
        }
        return instance;
    }

    public static <T> void doIfNotNull(T instance, Consumer<T> action) {
        if (instance != null) {
            action.accept(instance);
        }
    }

    public static <T> void silently(T instance, Consumer<T> action) {
        if (instance != null) {
            try {
                action.accept(instance);
            } catch (Exception e) {
                // silent, remember
            }
        }
    }

    public static String randomHex(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(Integer.toString(ThreadLocalRandom.current().nextInt(0, 16), 16));
        }
        return sb.toString();
    }

}
