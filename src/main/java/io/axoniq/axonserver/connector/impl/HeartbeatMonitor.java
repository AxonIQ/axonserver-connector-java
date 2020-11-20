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

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.grpc.control.Heartbeat;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitor dealing with all the logic around sending out heartbeats.
 */
public class HeartbeatMonitor {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatMonitor.class);

    private static final PlatformInboundInstruction HEARTBEAT_MESSAGE =
            PlatformInboundInstruction.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build();

    static Clock clock = Clock.systemUTC();

    private final ScheduledExecutorService executor;
    private final HeartbeatSender sender;
    private final Runnable onHeartbeatMissed;

    private final AtomicLong nextHeartbeatDeadline = new AtomicLong();
    private final AtomicLong nextHeartbeat = new AtomicLong();
    private final AtomicLong timeout = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong interval = new AtomicLong(Long.MAX_VALUE);
    private final AtomicInteger taskId = new AtomicInteger();

    /**
     * Constructs a {@link HeartbeatMonitor}.
     *
     * @param executor          the {@link ScheduledExecutorService} used to schedule operations to validate if a
     *                          heartbeat should be send with the given {@code heartbeatSender}
     * @param heartbeatSender   the {@link HeartbeatSender} used to send heartbeats with
     * @param onHeartbeatMissed operation to perform if a heartbeat has been missed. Can be used to force a reconnect of
     *                          a channel for example
     */
    public HeartbeatMonitor(ScheduledExecutorService executor,
                            HeartbeatSender heartbeatSender,
                            Runnable onHeartbeatMissed) {
        this.executor = executor;
        this.sender = heartbeatSender;
        this.onHeartbeatMissed = onHeartbeatMissed;
    }

    /**
     * Turn on heartbeat sending by this {@link HeartbeatMonitor}.
     *
     * @param interval the interval at which heartbeats occur. Will use a minimal value of {@code 500} milliseconds to
     *                 reschedule heartbeat validation
     * @param timeout  the timeout within which this monitor expects responses to the dispatched heartbeats. If this
     *                 timeout is hit, the given {@code onHeartbeatMissed} on construction will be called
     * @param timeUnit the {@link TimeUnit} used to define in which time frame both the given {@code interval} and
     *                 {@code timeout} reside. Will be used to change both values to their representative in
     *                 milliseconds
     */
    public void enableHeartbeat(long interval, long timeout, TimeUnit timeUnit) {
        this.interval.set(timeUnit.toMillis(interval));
        this.timeout.set(timeUnit.toMillis(timeout));
        long now = clock.millis();
        nextHeartbeat.set(now);
        nextHeartbeatDeadline.set(now + timeUnit.toMillis(timeout));
        int task = taskId.incrementAndGet();
        executor.execute(() -> checkAndReschedule(task));
    }

    /**
     * Turn off heartbeat sending by this {@link HeartbeatMonitor}.
     */
    public void disableHeartbeat() {
        this.interval.set(Long.MAX_VALUE);
        this.nextHeartbeatDeadline.set(Long.MAX_VALUE);
        taskId.incrementAndGet();
    }

    private void checkAndReschedule(int task) {
        if (task == taskId.get()) {
            // heartbeats should not be considered valid when a change was made
            checkBeat();
            long delay = Math.min(interval.get(), 1000);
            logger.debug("Heartbeat status checked. Scheduling next heartbeat verification in {}ms", delay);
            executor.schedule(() -> checkAndReschedule(task), delay, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Pause the process of sending out heartbeats by this monitor.
     */
    public void pause() {
        long currentInterval = this.interval.get();
        long currentTimeout = this.timeout.get();
        if (currentInterval != Long.MAX_VALUE || currentTimeout != Long.MAX_VALUE) {
            taskId.incrementAndGet();
        }
    }

    /**
     * Resume the process of sending out heartbeats by this monitor.
     */
    public void resume() {
        long currentInterval = this.interval.get();
        long currentTimeout = this.timeout.get();
        if (currentInterval != Long.MAX_VALUE && currentTimeout != Long.MAX_VALUE) {
            enableHeartbeat(currentInterval, currentTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private void checkBeat() {
        long now = clock.millis();
        long nextDeadline = nextHeartbeatDeadline.get();
        if (nextDeadline <= now) {
            logger.info("Did not receive heartbeat acknowledgement within {}ms", this.timeout.get());
            onHeartbeatMissed.run();
            nextHeartbeatDeadline.compareAndSet(nextDeadline, now + this.interval.get());
        }
        if (planNextBeat(now, this.interval.get())) {
            long currentInterval = this.interval.get();
            long beatTimeout = this.timeout.get();
            sender.sendHeartbeat().whenComplete((r, e) -> {

                boolean success = e == null
                        || (e instanceof AxonServerException && ((AxonServerException) e).getErrorCategory() == ErrorCategory.UNSUPPORTED_INSTRUCTION);
                // if AxonServer indicates it doesn't know this instruction, we have at least reached it.
                // We can assume the connection is alive
                if (success) {
                    if (currentInterval != Long.MAX_VALUE) {
                        long newDeadline = nextHeartbeatDeadline.updateAndGet(
                                currentDeadline -> Math.max(now + beatTimeout + currentInterval, currentDeadline)
                        );
                        if (logger.isDebugEnabled()) {
                            logger.debug("Heartbeat Acknowledgement received. Extending deadline to {}",
                                         Instant.ofEpochMilli(newDeadline));
                        }
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("Heartbeat Acknowledgment received.");
                    }
                }
            });
        }
    }

    /**
     * Calculates the time for the next heartbeat based on the most recent heartbeat time and the interval, indicating
     * whether the time has been reached to send a heartbeat message.
     *
     * @param currentTime the current timestamp
     * @param interval    the interval at which heartbeats occur
     * @return whether or not the time for a new heartbeat has elapsed
     */
    private boolean planNextBeat(long currentTime, long interval) {
        return nextHeartbeat.getAndAccumulate(
                interval, (next, currentInterval) -> next <= currentTime ? currentTime + currentInterval : next
        ) <= currentTime;
    }

    /**
     * Handler of {@link PlatformInboundInstruction} requesting a heartbeat from this connector. The given {@link
     * ReplyChannel} is used to send responding heartbeat message with.
     *
     * @param reply the {@link ReplyChannel} to send a heartbeat reply message over
     */
    public void handleIncomingBeat(ReplyChannel<PlatformInboundInstruction> reply) {
        long now = clock.millis();
        long currentInterval = this.interval.get();
        // receiving a heartbeat from Server is equivalent to receiving an acknowledgement
        planNextBeat(now, currentInterval);
        nextHeartbeatDeadline.updateAndGet(current -> Math.max(now + currentInterval, current));
        try {
            reply.send(HEARTBEAT_MESSAGE);
        } finally {
            reply.complete();
        }
    }
}
