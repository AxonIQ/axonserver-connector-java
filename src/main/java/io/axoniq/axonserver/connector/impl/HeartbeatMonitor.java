package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.grpc.control.Heartbeat;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class HeartbeatMonitor {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatMonitor.class);

    static Clock clock = Clock.systemUTC();

    private final ScheduledExecutorService executor;
    private final Supplier<CompletableFuture<?>> sender;
    private final Runnable onHeartbeatMissed;
    private final AtomicLong nextHeartbeatTimeout = new AtomicLong();
    private final AtomicLong nextHeartbeat = new AtomicLong();
    private final AtomicLong timeout = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong interval = new AtomicLong(Long.MAX_VALUE);
    private final AtomicInteger taskId = new AtomicInteger();

    public HeartbeatMonitor(ScheduledExecutorService executor,
                            Supplier<CompletableFuture<?>> heartbeatSender,
                            Runnable onHeartbeatMissed) {
        this.executor = executor;
        this.sender = heartbeatSender;
        this.onHeartbeatMissed = onHeartbeatMissed;
    }

    public void enableHeartbeat(long interval, long timeout, TimeUnit timeUnit) {
        this.interval.set(timeUnit.toMillis(interval));
        this.timeout.set(timeUnit.toMillis(timeout));
        long now = clock.millis();
        nextHeartbeat.set(now);
        nextHeartbeatTimeout.set(now + timeUnit.toMillis(timeout));
        int task = taskId.incrementAndGet();
        executor.execute(() -> checkAndReschedule(task));
    }

    public void disableHeartbeat() {
        this.interval.set(Long.MAX_VALUE);
        this.nextHeartbeatTimeout.set(Long.MAX_VALUE);
        taskId.incrementAndGet();
    }

    public void checkAndReschedule(int task) {
        if (task == taskId.get()) {
            // heartbeats should not be considered valid when a change was made
            checkBeat();
            executor.schedule(() -> checkAndReschedule(task), 500, TimeUnit.MILLISECONDS);
        }
    }

    public void pause() {
        long interval = this.interval.get();
        long timeout = this.timeout.get();
        if (interval != Long.MAX_VALUE || timeout != Long.MAX_VALUE) {
            taskId.incrementAndGet();
        }
    }

    public void resume() {
        long interval = this.interval.get();
        long timeout = this.timeout.get();
        if (interval != Long.MAX_VALUE && timeout != Long.MAX_VALUE) {
            enableHeartbeat(interval, timeout, TimeUnit.MILLISECONDS);
        }
    }

    public void checkBeat() {
        long now = clock.millis();
        long nextTimeout = nextHeartbeatTimeout.get();
        if (nextTimeout <= now) {
            logger.info("Did not receive heartbeat acknowledgement within {}ms", this.timeout.get());
            onHeartbeatMissed.run();
            nextHeartbeatTimeout.compareAndSet(nextTimeout, now + interval.get());
        }
        if (nextHeartbeat.getAndAccumulate(interval.get(),
                                           (next, interval) -> next <= now ? now + interval : next) <= now) {
            sender.get().whenComplete((r, e) -> {
                if (e == null) {
                    long interval = this.interval.get();
                    if (interval != Long.MAX_VALUE) {
                        nextHeartbeatTimeout.updateAndGet(current -> Math.max(nextTimeout + interval, current));
                    }
                }
            });
        }
    }

    public void handleIncomingBeat(PlatformOutboundInstruction msg, ReplyChannel<PlatformInboundInstruction> reply) {
        long now = clock.millis();
        long interval = this.interval.get();
        nextHeartbeatTimeout.updateAndGet(current -> Math.max(now + interval, current));
        try {
            reply.send(PlatformInboundInstruction.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
        } finally {
            reply.complete();
        }
    }
}
