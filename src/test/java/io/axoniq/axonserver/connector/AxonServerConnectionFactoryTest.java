/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector;

import io.axoniq.axonserver.connector.impl.ServerAddress;
import org.junit.jupiter.api.*;
import org.testng.reporters.Files;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link AxonServerConnectionFactory}.
 *
 * @author Steven van Beelen
 */
class AxonServerConnectionFactoryTest {

    private static final String TEST_COMPONENT_NAME = "some-component-name";
    private static final String TEST_CONTEXT = "some-context";

    private final ByteArrayOutputStream systemOut = new ByteArrayOutputStream();
    private final PrintStream originalSystemOut = System.out;

    private String downloadMessage;
    private AxonServerConnectionFactory testSubject;

    @BeforeEach
    void setUp() throws IOException {
        System.setOut(new PrintStream(systemOut));

        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("axonserver_download.txt")) {
            // noinspection DataFlowIssue
            downloadMessage = Files.readFile(inputStream);
        }
    }

    @AfterEach
    void tearDown() {
        if (testSubject != null) {
            testSubject.shutdown();
        }
        System.setOut(originalSystemOut);
        System.clearProperty("axon.axonserver.suppressDownloadMessage");
    }

    @Test
    void downloadMessageIsNotSuppressedForDefaultServerAddress() {
        assertFalse(downloadMessage.isEmpty());

        testSubject = AxonServerConnectionFactory.forClient(TEST_COMPONENT_NAME)
                                                                             .routingServers(ServerAddress.DEFAULT)
                                                                             .build();

        testSubject.connect(TEST_CONTEXT);
        String replace = systemOut.toString().replace("\r\n", "\n");
        assertTrue(replace.contains(downloadMessage));
    }

    @Test
    void downloadMessageIsSuppressedForNoneDefaultServerAddress() {
        assertFalse(downloadMessage.isEmpty());

        ServerAddress customAddress = new ServerAddress("my-host", 4218);
        testSubject = AxonServerConnectionFactory.forClient(TEST_COMPONENT_NAME)
                                                                             .routingServers(customAddress)
                                                                             .build();

        testSubject.connect(TEST_CONTEXT);
        String replace = systemOut.toString().replace("\r\n", "\n");
        assertFalse(replace.contains(downloadMessage));
    }

    @Test
    void downloadMessageIsSuppressedWhenSuppressionPropertyIsSet() {
        assertFalse(downloadMessage.isEmpty());

        System.setProperty("axon.axonserver.suppressDownloadMessage", "true");
        testSubject = AxonServerConnectionFactory.forClient(TEST_COMPONENT_NAME)
                                                                             .routingServers(ServerAddress.DEFAULT)
                                                                             .build();

        testSubject.connect(TEST_CONTEXT);
        String replace = systemOut.toString().replace("\r\n", "\n");
        assertFalse(replace.contains(downloadMessage));
    }
}