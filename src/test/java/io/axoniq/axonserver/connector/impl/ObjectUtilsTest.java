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