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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObjectUtilsTest {

    @Test
    void testHexGenerationUsesAllChars() {
        String actual = ObjectUtils.randomHex(5000);

        String chars = "0123456789abcdef";
        for (int i = 0; i < chars.length(); i++) {
            String ch = chars.substring(i, i + 1);
            assertTrue(actual.contains(ch), "Random hex didn't pick a " + ch);
        }
        assertTrue(actual.matches("^[0-9a-f]+$"));
    }

    @Test
    void testRandomHexGeneration() {
        String random1 = ObjectUtils.randomHex(10);
        String random2 = ObjectUtils.randomHex(5);
        String random3 = ObjectUtils.randomHex(7);

        assertEquals(10, random1.length());
        assertEquals(5, random2.length());
        assertEquals(7, random3.length());

        assertTrue(random1.matches("^[0-9a-f]+$"));
        assertTrue(random2.matches("^[0-9a-f]+$"));
        assertTrue(random3.matches("^[0-9a-f]+$"));
    }
}