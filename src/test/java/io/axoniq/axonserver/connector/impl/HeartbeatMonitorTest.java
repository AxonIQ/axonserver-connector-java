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

import io.axoniq.axonserver.grpc.InstructionAck;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class HeartbeatMonitorTest {

    private ScheduledExecutorService executor;
    private List<CompletableFuture<?>> scheduledBeats = new CopyOnWriteArrayList<>();
    private Runnable nextCheckBeat;
    private long now;
    private AtomicBoolean trigger;
    private HeartbeatMonitor testSubject;

    @BeforeEach
    void setUp() {
        now = 0;
        elapseTime(0);
        executor = mock(ScheduledExecutorService.class);
        doAnswer(inv -> {
            nextCheckBeat = () -> {nextCheckBeat = null ; inv.getArgumentAt(0, Runnable.class).run();};
            return null;
        }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        doAnswer(inv -> nextCheckBeat = () -> {nextCheckBeat = null ; inv.getArgumentAt(0, Runnable.class).run();}).when(executor).execute(any(Runnable.class));
        HeartbeatSender mockSender = () -> {
            CompletableFuture<InstructionAck> newBeat = new CompletableFuture<>();
            scheduledBeats.add(newBeat);
            return newBeat;
        };


        trigger = new AtomicBoolean();
        testSubject = new HeartbeatMonitor(executor, mockSender, () -> trigger.set(true));
    }

    private void elapseTime(long millis) {
        now += millis;
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
        nextCheckBeat.run();
        assertEquals(1, this.scheduledBeats.size());

        nextCheckBeat.run();
        assertEquals(1, this.scheduledBeats.size());
        // we didn't schedule a new beat, because the required 1000ms haven't elapsed yet
        elapseTime(1000);
        nextCheckBeat.run();

        assertEquals(2, this.scheduledBeats.size());
        // a second has passed, and we didn't confirm the first heartbeat
        assertTrue(trigger.get());
    }

    @Test
    void testMissingABeatSetsCorrectDeadline() {
        testSubject.enableHeartbeat(100, 500, TimeUnit.MILLISECONDS);

        for (int i = 0; i < 4; i++) {
            elapseTime(100);
            nextCheckBeat.run();
        }

        assertEquals(4, scheduledBeats.size());
        assertFalse(trigger.get());
        // completing this beat should set the deadline at 600ms
        scheduledBeats.get(3).complete(null);
        elapseTime(100);
        nextCheckBeat.run();
        assertEquals(5, scheduledBeats.size());
        assertFalse(trigger.get());
        elapseTime(99);
        nextCheckBeat.run();
        assertFalse(trigger.get());
        elapseTime(1);
        nextCheckBeat.run();
        assertTrue(trigger.get());
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

        nextCheckBeat.run();
        elapseTime(400);
        nextCheckBeat.run();
        assertFalse(trigger.get());
        scheduledBeats.get(0).complete(null);
        elapseTime(100);
        nextCheckBeat.run();
        assertFalse(trigger.get());
        elapseTime(500);
        assertEquals(1, scheduledBeats.size());
        nextCheckBeat.run();
        assertEquals(2, scheduledBeats.size());
        elapseTime(499);
        nextCheckBeat.run();
        assertFalse(trigger.get());
        elapseTime(1);
        nextCheckBeat.run();
        assertTrue(trigger.get());
    }

    @Test
    void testDisablingHeartbeatIgnoresPreviousDeadline() {
        testSubject.enableHeartbeat(100, 50, TimeUnit.MILLISECONDS);
        nextCheckBeat.run();
        assertEquals(1, scheduledBeats.size());
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
        assertEquals(1, scheduledBeats.size());
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
}