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
    private final Runnable onConnectionCorrupted;

    private final AtomicLong nextHeartbeatDeadline = new AtomicLong();
    private final AtomicLong nextHeartbeat = new AtomicLong();
    private final AtomicLong timeout = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong interval = new AtomicLong(Long.MAX_VALUE);
    private final AtomicInteger taskId = new AtomicInteger();

    /**
     * Constructs a {@link HeartbeatMonitor}.
     *
     * @param executor              the {@link ScheduledExecutorService} used to schedule operations to validate if a
     *                              heartbeat should be send with the given {@code heartbeatSender}
     * @param heartbeatSender       the {@link HeartbeatSender} used to send heartbeats with
     * @param onConnectionCorrupted operation to perform if a heartbeat has been missed. Can be used to force a
     *                              reconnect of a channel for example
     */
    public HeartbeatMonitor(ScheduledExecutorService executor,
                            HeartbeatSender heartbeatSender,
                            Runnable onConnectionCorrupted) {
        this.executor = executor;
        this.sender = heartbeatSender;
        this.onConnectionCorrupted = onConnectionCorrupted;
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
        if (task != taskId.get()) {
            // heartbeats should not be considered valid when a change was made
            return;
        }
        long delay = Math.min(interval.get(), 1000);
        try {
            checkBeatDeadline();
            sendBeatIfTimeElapsed();
            logger.debug("Heartbeat status checked. Scheduling next heartbeat verification in {}ms", delay);
        } catch (Exception e) {
            logger.warn("Was unable to send heartbeat due to exception", e);
        } finally {
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

    /**
     * Send a heartbeat if the next time a heartbeat should be sent has passed.
     */
    private void sendBeatIfTimeElapsed() {
        if (shouldSendBeat()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Sending heartbeat due to elapsed next beat interval.");
            }
            sender.sendHeartbeat().whenComplete((r, e) -> handleHeartbeatCallResult(e));

            Instant newNextHeartbeatTime = extendNextHeartbeatTime();
            if (logger.isDebugEnabled()) {
                logger.debug("Next heartbeat has been planned for {} due to sent heartbeat", newNextHeartbeatTime);
            }
        }
    }

    /**
     * Checks whether the deadline has elapsed, marking the connection as dead and calling the provided
     * {@link Runnable}.
     */
    private void checkBeatDeadline() {
        long now = clock.millis();
        long nextDeadline = nextHeartbeatDeadline.get();
        if (nextDeadline <= now) {
            logger.info("Did not receive heartbeat acknowledgement within {}ms", this.timeout.get());
            onConnectionCorrupted.run();
            extendHeartbeatDeadline();
        }
    }

    /**
     * Handles the result of the heartbeat call the client does with the server. In case of success, we extend the
     * deadline. In case of error, we mark the connection as corrupted.
     *
     * @param e Optional error result of the heartbeat call initiated by this client.
     */
    private void handleHeartbeatCallResult(Throwable e) {
        boolean success = e == null || isUnsupportedInstructionError(e);
        if (!success) {
            logger.debug("Heartbeat call resulted in an error.", e);
            onConnectionCorrupted.run();
            return;
        }
        boolean heartbeatsDisabled = interval.get() == Long.MAX_VALUE;
        if (heartbeatsDisabled) {
            if (logger.isDebugEnabled()) {
                logger.debug("Heartbeat Acknowledgment received but heartbeats were disabled.");
            }
            return;
        }

        Instant extendedDeadline = extendHeartbeatDeadline();
        if (logger.isDebugEnabled()) {
            logger.debug("Heartbeat call succeeded and extended deadline to {}", extendedDeadline);
        }
    }

    /**
     * Checks whether the given error is an Unsupported instruction error. If we get this while sending a heartbeat
     * call, we are talking with an older version of the server. However, reaching it in the first place means the
     * connection is alive, so we can count this as a valid heartbeat.
     */
    private boolean isUnsupportedInstructionError(Throwable e) {
        return e instanceof AxonServerException
                && ((AxonServerException) e).getErrorCategory() == ErrorCategory.UNSUPPORTED_INSTRUCTION;
    }

    /**
     * Whether a beat should be sent, based on the current time and the next scheduled heartbeat.
     */
    private boolean shouldSendBeat() {
        long now = clock.millis();
        return nextHeartbeat.get() <= now;
    }

    /**
     * Set the next heartbeat send time to the configured interval in the future.
     *
     * @return The new time the next heartbeat will be sent
     */
    private Instant extendNextHeartbeatTime() {
        long now = clock.millis();
        long newNextHeartbeat = nextHeartbeat.updateAndGet(current -> now + interval.get());
        return Instant.ofEpochMilli(newNextHeartbeat);
    }

    /**
     * Sets the heartbeat deadline to a new value, based on the current time and provided configuration.
     *
     * @return The new heartbeat deadline, after which we can assume the connection is dead.
     */
    private Instant extendHeartbeatDeadline() {
        long now = clock.millis();
        long newDeadline = nextHeartbeatDeadline.updateAndGet(
                currentDeadline -> Math.max(now + timeout.get() + interval.get(), currentDeadline)
        );
        return Instant.ofEpochMilli(newDeadline);
    }

    /**
     * Handler of {@link PlatformInboundInstruction} requesting a heartbeat from this connector. The given
     * {@link ReplyChannel} is used to send responding heartbeat message with.
     *
     * @param reply the {@link ReplyChannel} to send a heartbeat reply message over
     */
    public void handleIncomingBeat(ReplyChannel<PlatformInboundInstruction> reply) {
        Instant newHeartbeatTime = extendNextHeartbeatTime();
        Instant extendedDeadline = extendHeartbeatDeadline();

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Received heartbeat call from server, extending deadline to {} and planned next heartbeat for {}",
                    extendedDeadline,
                    newHeartbeatTime);
        }

        try {
            reply.send(HEARTBEAT_MESSAGE);
        } finally {
            reply.complete();
        }
    }
}
