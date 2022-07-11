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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

class HeartbeatMonitorTest {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatMonitorTest.class);

    private ScheduledExecutorService executor;
    private final List<CompletableFuture<?>> sentBeats = new CopyOnWriteArrayList<>();
    private Runnable nextCheckBeat;
    private long now;
    private AtomicBoolean trigger;
    private HeartbeatMonitor testSubject;

    @BeforeEach
    void setUp() {
        Configurator.setAllLevels("io.axoniq.axonserver.connector.impl.HeartbeatMonitor", Level.ALL);
        elapseTime(0);
        executor = mock(ScheduledExecutorService.class);
        doAnswer(inv -> {
            nextCheckBeat = () -> {nextCheckBeat = null ; inv.getArgumentAt(0, Runnable.class).run();};
            return null;
        }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        doAnswer(inv -> nextCheckBeat = () -> {nextCheckBeat = null ; inv.getArgumentAt(0, Runnable.class).run();}).when(executor).execute(any(Runnable.class));
        HeartbeatSender mockSender = () -> {
            CompletableFuture<Void> newBeat = new CompletableFuture<>();
            sentBeats.add(newBeat);
            logger.info("Sent heartbeat at {}ms", now);
            return newBeat;
        };


        trigger = new AtomicBoolean();
        testSubject = new HeartbeatMonitor(executor, mockSender, () -> trigger.set(true));
    }

    private void elapseTime(long millis) {
        now += millis;
        logger.info("Elapsed {}ms, now is at {}ms", millis, now);
        HeartbeatMonitor.clock = Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC);
    }

    @AfterAll
    static void cleanUp() {
        HeartbeatMonitor.clock = Clock.systemUTC();
    }

    @Test
    void testHeartbeatSentAtInterval() {
        testSubject.enableHeartbeat(1000, 500, TimeUnit.MILLISECONDS);

        assertNotNull(nextCheckBeat, "expected next beat & check to be scheduled");
        elapseRunAndExpect(0, false, true);
        elapseRunAndExpect(0, false, false);
        elapseRunAndExpect(1000, true, true);
    }

    @Test
    void testMissingABeatSetsCorrectDeadline() {
        testSubject.enableHeartbeat(100, 500, TimeUnit.MILLISECONDS);

        for (int i = 0; i < 4; i++) {
            elapseRunAndExpect(100, false, true);
        }

        // completing this beat should set the deadline at 1000ms
        sentBeats.get(3).complete(null);

        elapseRunAndExpect(599, false, true);
        elapseRunAndExpect(1, true, false);
    }

    @Test
    void testPausingPreventsTriggers() {
        testSubject.enableHeartbeat(100, 500, TimeUnit.MILLISECONDS);

        elapseTime(100);
        nextCheckBeat.run();

        testSubject.pause();

        elapseTime(1000);
        // the next beat was already scheduled
        nextCheckBeat.run();
        assertFalse(trigger.get());

        assertNull(nextCheckBeat, "Did not expect scheduling of heartbeat after pause");

    }

    @Test
    void testResumeRestartsTimers() {
        testSubject.enableHeartbeat(100, 500, TimeUnit.MILLISECONDS);

        elapseTime(100);
        testSubject.pause();
        nextCheckBeat.run();
        assertFalse(trigger.get());

        assertNull(nextCheckBeat, "Did not expect scheduling of heartbeat after pause");

        elapseTime(1000);
        testSubject.resume();

        nextCheckBeat.run();
        elapseTime(499);
        nextCheckBeat.run();
        assertFalse(trigger.get());
        elapseTime(1);
        nextCheckBeat.run();

        assertTrue(trigger.get());
    }

    @Test
    void testHeartbeatTriggeredWhenDeadlineElapses() {
        testSubject.enableHeartbeat(1000, 500, TimeUnit.MILLISECONDS);

        elapseRunAndExpect(0, false, true);

        elapseRunAndExpect(400, false, false);


        // First hearbeat completes at 400ms
        sentBeats.get(0).complete(null);

        // Should not send anything, since the interval did not elapse (currently planned at 1400ms)
        elapseRunAndExpect(100, false, false);

        // Should send beat
        elapseRunAndExpect(500, false, true);

        // Should not trigger yet
        elapseRunAndExpect(899, false, false);

        // Should trigger now, since heartbeat came back at 400ms, adding 500ms + 1000ms of deadline. It's now 1900
        elapseRunAndExpect(1, true, false);
    }

    @Test
    void testReceivingHeartbeatsExtendsDeadline() {
        testSubject.enableHeartbeat(1000, 500, TimeUnit.MILLISECONDS);
        elapseRunAndExpect(0, false, true);

        elapseRunAndExpect(400, false, false);
        testSubject.handleIncomingBeat(mock(ForwardingReplyChannel.class));

        elapseRunAndExpect(999, false, false);
        elapseRunAndExpect(1, false, true);
    }


    @Test
    void testDisablingHeartbeatIgnoresPreviousDeadline() {
        testSubject.enableHeartbeat(100, 50, TimeUnit.MILLISECONDS);
        nextCheckBeat.run();
        assertEquals(1, sentBeats.size());
        testSubject.disableHeartbeat();
        elapseTime(50);
        nextCheckBeat.run();
        assertNull(nextCheckBeat);
        assertFalse(trigger.get());
    }

    @Test
    void testReschedulingHeartbeatIgnoresPreviousDeadline() {
        testSubject.enableHeartbeat(100, 50, TimeUnit.MILLISECONDS);
        nextCheckBeat.run();
        assertEquals(1, sentBeats.size());
        testSubject.enableHeartbeat(1000, 500, TimeUnit.MILLISECONDS);
        elapseTime(50);
        nextCheckBeat.run();
        assertFalse(trigger.get());
        elapseTime(449);
        nextCheckBeat.run();
        assertFalse(trigger.get());
        elapseTime(1);
        nextCheckBeat.run();
        assertTrue(trigger.get());
    }

    private int previousBeatCount = 0;

    private void elapseRunAndExpect(long ms, boolean triggered, boolean beatSent) {
        elapseTime(ms);
        nextCheckBeat.run();
        assertEquals(triggered, trigger.get());
        if (beatSent) {
            assertEquals(previousBeatCount + 1, sentBeats.size());
            previousBeatCount++;
        } else {
            assertEquals(previousBeatCount, sentBeats.size());
        }
    }
}
