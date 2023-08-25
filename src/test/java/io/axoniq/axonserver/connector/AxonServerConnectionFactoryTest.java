/*
 * Copyright (c) 2023. AxonIQ
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
        System.setOut(originalSystemOut);
        System.clearProperty("axon.axonserver.suppressDownloadMessage");
    }

    @Test
    void downloadMessageIsNotSuppressedForDefaultServerAddress() {
        assertFalse(downloadMessage.isEmpty());

        AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient(TEST_COMPONENT_NAME)
                                                                             .routingServers(ServerAddress.DEFAULT)
                                                                             .build();

        testSubject.connect(TEST_CONTEXT);
        assertTrue(systemOut.toString().contains(downloadMessage));
    }

    @Test
    void downloadMessageIsSuppressedForNoneDefaultServerAddress() {
        assertFalse(downloadMessage.isEmpty());

        ServerAddress customAddress = new ServerAddress("my-host", 4218);
        AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient(TEST_COMPONENT_NAME)
                                                                             .routingServers(customAddress)
                                                                             .build();

        testSubject.connect(TEST_CONTEXT);

        assertFalse(systemOut.toString().contains(downloadMessage));
    }

    @Test
    void downloadMessageIsSuppressedWhenSuppressionPropertyIsSet() {
        assertFalse(downloadMessage.isEmpty());

        System.setProperty("axon.axonserver.suppressDownloadMessage", "true");
        AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient(TEST_COMPONENT_NAME)
                                                                             .routingServers(ServerAddress.DEFAULT)
                                                                             .build();

        testSubject.connect(TEST_CONTEXT);

        assertFalse(systemOut.toString().contains(downloadMessage));
    }
}