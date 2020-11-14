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
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.grpc.ConnectivityState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;
import static io.axoniq.axonserver.connector.impl.ObjectUtils.hasLength;

/**
 * Abstract class representing a channel with AxonServer.
 */
public abstract class AbstractAxonServerChannel<OUT> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractAxonServerChannel.class);

    private final ClientIdentification clientIdentification;
    private final ScheduledExecutorService executor;
    private final AxonServerManagedChannel channel;
    private final ConcurrentMap<String, CompletableFuture<Void>> instructions = new ConcurrentHashMap<>();

    /**
     * Instantiate an {@link AbstractAxonServerChannel}.
     *
     * @param executor                 a {@link ScheduledExecutorService} used to schedule reconnections
     * @param axonServerManagedChannel the {@link AxonServerManagedChannel} used to vconnect to AxonServer
     */
    protected AbstractAxonServerChannel(ClientIdentification clientIdentification,
                                        ScheduledExecutorService executor,
                                        AxonServerManagedChannel axonServerManagedChannel) {
        this.clientIdentification = clientIdentification;
        this.executor = executor;
        this.channel = axonServerManagedChannel;
    }

    protected CompletableFuture<Void> sendInstruction(OUT instruction, Function<OUT, String> instructionIdProvider, StreamObserver<OUT> outboundStream) {
        CompletableFuture<Void> ack = new CompletableFuture<>();
        String instructionId = instructionIdProvider.apply(instruction);
        if (hasLength(instructionId)) {
            instructions.put(instructionId, ack);
            ack.whenComplete((r, e) -> instructions.remove(instructionId, ack));
        }
        doIfNotNull(outboundStream, s -> s.onNext(instruction))
                .orElse(() -> ack.completeExceptionally(new AxonServerException(ErrorCategory.INSTRUCTION_ACK_ERROR,
                                                                                "Unable to send instruction: no connection to AxonServer",
                                                                                clientIdentification.getClientId())));
        if (!hasLength(instructionId)) {
            ack.complete(null);
        }
        return ack;
    }

    protected void processAck(InstructionAck ack) {
        CompletableFuture<Void> future = instructions.remove(ack.getInstructionId());
        if (future != null) {
            if (ack.getSuccess()) {
                future.complete(null);
            } else {
                future.completeExceptionally(new AxonServerException(ack.getError()));
            }
        }

    }


    /**
     * Schedule an attempt to reconnect with AxonServer. Depending on the {@code disconnectReason}, the reconnect
     * attempt will be executed within 500ms or 5000ms.
     *
     * @param disconnectReason The reason why the previous connection failed.
     */
    protected void scheduleReconnect(Throwable disconnectReason) {
        instructions.keySet().forEach(
                k -> doIfNotNull(instructions.remove(k), f -> f.completeExceptionally(disconnectReason))
        );

        switch (Status.fromThrowable(disconnectReason).getCode()) {
            case NOT_FOUND:
            case PERMISSION_DENIED:
            case UNIMPLEMENTED:
            case UNAUTHENTICATED:
            case FAILED_PRECONDITION:
            case INVALID_ARGUMENT:
            case RESOURCE_EXHAUSTED:
                scheduleReconnect(5000);
                break;
            default:
                scheduleReconnect(500);
                break;
        }
    }


    /**
     * Schedule an immediate attempt to reconnect with AxonServer.
     */
    protected void scheduleImmediateReconnect() {
        logger.debug("Scheduling immediate reconnect");
        scheduleReconnect(0);
    }

    private void scheduleReconnect(int delay) {
        try {
            executor.schedule(() -> {
                ConnectivityState connectivityState = channel.getState(delay == 0);
                if (connectivityState == ConnectivityState.READY) {
                    connect();
                } else {
                    logger.debug("No connection to AxonServer available. Scheduling next attempt in 500ms");
                    scheduleReconnect(500);
                }
            }, delay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            logger.info("Ignoring reconnect request, as connector is being shut down.");
        }
    }

    /**
     * Connect this channel with AxonServer.
     */
    public abstract void connect();

    /**
     * Reconnect this channel with AxonServer.
     */
    public abstract void reconnect();

    /**
     * Disconnect this channel from AxonServer.
     */
    public abstract void disconnect();

    /**
     * Validate whether this channel has all required streams connected with AxonServer. If the state of the channel
     * does not require any active streams, it is considered ready and will return {@code true}.
     *
     * @return {@code true} if this channel is connected with AxonServer or does not require any active connections,
     * {@code false} otherwise
     */
    public abstract boolean isReady();
}
