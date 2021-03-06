/*
 * Copyright (c) 2020-2021. AxonIQ
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

/**
 * Utility class to perform assertion on method parameters.
 *
 * @author Allard Buijze
 * @since 4.5
 */
public class AssertUtils {

    private AssertUtils() {
    }

    /**
     * Assert that the given {@code condition} is true, or otherwise throws an {@link IllegalArgumentException} with
     * given {@code message}.
     *
     * @param condition The condition to validate
     * @param message   The message in the exception
     */
    public static void assertParameter(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
