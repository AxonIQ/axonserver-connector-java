package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.grpc.InstructionAck;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface HeartbeatSender {
    CompletableFuture<InstructionAck> sendHeartbeat();
}
