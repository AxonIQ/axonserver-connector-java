/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.control.ProcessorInstructionHandler;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.grpc.ClientCall;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that the periodic sending of event processor info keeps running when sending the info of a processor
 * fails.
 */
class ControlChannelImplProcessorInfoTest {

    private static final long UPDATE_FREQUENCY_MS = 10;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final List<PlatformInboundInstruction> sentMessages = new CopyOnWriteArrayList<>();
    private final AtomicInteger sendFailuresToSimulate = new AtomicInteger();
    private ScheduledExecutorService executor;
    private ControlChannelImpl testSubject;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
        AxonServerManagedChannel channel = mock(AxonServerManagedChannel.class);
        ClientCall call = mock(ClientCall.class);
        when(channel.newCall(any(), any())).thenReturn(call);
        when(call.isReady()).thenReturn(true);
        doAnswer(invocation -> {
            PlatformInboundInstruction message = invocation.getArgument(0);
            if (message.hasEventProcessorInfo() && sendFailuresToSimulate.getAndUpdate(i -> Math.max(0, i - 1)) > 0) {
                throw new IllegalStateException("call was half-closed");
            }
            sentMessages.add(message);
            return null;
        }).when(call).sendMessage(any());

        testSubject = new ControlChannelImpl(ClientIdentification.newBuilder()
                                                                 .setClientId("client")
                                                                 .setComponentName("component")
                                                                 .build(),
                                             "context",
                                             executor,
                                             channel,
                                             UPDATE_FREQUENCY_MS,
                                             () -> {
                                             });
        testSubject.connect();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void processorInfoIsStillSentAfterSupplierThrowsException() {
        AtomicInteger invocations = new AtomicInteger();
        testSubject.registerEventProcessor("processor", () -> {
            // the first invocation happens on registration, the second (scheduled) one fails
            if (invocations.incrementAndGet() == 2) {
                throw new IllegalStateException("simulated failure building processor info");
            }
            return processorInfo("processor");
        }, mock(ProcessorInstructionHandler.class));

        await().atMost(TIMEOUT).until(() -> invocations.get() >= 5);
        assertTrue(sentInfoCount("processor") >= 4, "Processor info should be sent after the supplier failed");
    }

    @Test
    void processorInfoIsStillSentAfterSendingFails() {
        testSubject.registerEventProcessor("processor",
                                           () -> processorInfo("processor"),
                                           mock(ProcessorInstructionHandler.class));
        await().atMost(TIMEOUT).until(() -> sentInfoCount("processor") >= 1);

        sendFailuresToSimulate.set(1);
        await().atMost(TIMEOUT).until(() -> sendFailuresToSimulate.get() == 0);
        int sentBeforeRecovery = sentInfoCount("processor");

        await().atMost(TIMEOUT).until(() -> sentInfoCount("processor") >= sentBeforeRecovery + 3);
    }

    @Test
    void failingSupplierDoesNotBlockInfoOfOtherProcessors() {
        testSubject.registerEventProcessor("healthy",
                                           () -> processorInfo("healthy"),
                                           mock(ProcessorInstructionHandler.class));
        Supplier<EventProcessorInfo> alwaysFailing = () -> {
            throw new IllegalStateException("simulated failure building processor info");
        };
        testSubject.registerEventProcessor("failing", alwaysFailing, mock(ProcessorInstructionHandler.class));

        int sentBefore = sentInfoCount("healthy");
        await().atMost(TIMEOUT).until(() -> sentInfoCount("healthy") >= sentBefore + 3);
    }

    private static EventProcessorInfo processorInfo(String processorName) {
        return EventProcessorInfo.newBuilder().setProcessorName(processorName).build();
    }

    private int sentInfoCount(String processorName) {
        return (int) sentMessages.stream()
                                 .filter(PlatformInboundInstruction::hasEventProcessorInfo)
                                 .filter(m -> m.getEventProcessorInfo().getProcessorName().equals(processorName))
                                 .count();
    }
}
