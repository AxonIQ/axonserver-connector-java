package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class ForwardingReplyChannel<T> implements ReplyChannel<T> {

    private final AtomicBoolean ackSent = new AtomicBoolean(false);
    private final String instructionId;
    private final String clientId;
    private final StreamObserver<T> stream;
    private final Function<InstructionAck, T> ackBuilder;
    private final Runnable onConsumed;
    private final AtomicBoolean completed = new AtomicBoolean();

    public ForwardingReplyChannel(String instructionId,
                                  String clientId,
                                  StreamObserver<T> stream,
                                  Function<InstructionAck, T> ackBuilder,
                                  Runnable onConsumed) {
        this.instructionId = instructionId;
        this.clientId = clientId;
        this.stream = stream;
        this.ackBuilder = ackBuilder;
        this.onConsumed = onConsumed;
    }

    @Override
    public void send(T outboundMessage) {
        stream.onNext(outboundMessage);
    }

    @Override
    public void sendAck() {
        if (instructionId != null
                && !instructionId.isEmpty()
                && ackSent.compareAndSet(false, true)) {
            stream.onNext(ackBuilder.apply(InstructionAck.newBuilder().setInstructionId(instructionId).setSuccess(true).build()));
        }
    }

    @Override
    public void complete() {
        sendAck();
        markConsumed();
    }

    @Override
    public void completeWithError(ErrorMessage errorMessage) {
        sendNack(errorMessage);
        markConsumed();
    }

    @Override
    public void sendNack(ErrorMessage errorMessage) {
        if (instructionId != null && !instructionId.isEmpty() && ackSent.compareAndSet(false, true)) {
            InstructionAck ack = InstructionAck.newBuilder()
                                               .setInstructionId(instructionId)
                                               .setError(errorMessage == null ? ErrorMessage.getDefaultInstance() : errorMessage)
                                               .setSuccess(false)
                                               .build();
            stream.onNext(ackBuilder.apply(ack));
        }
    }

    @Override
    public void completeWithError(ErrorCategory errorCategory, String message) {
        completeWithError(ErrorMessage.newBuilder()
                                      .setErrorCode(errorCategory.errorCode())
                                      .setLocation(clientId)
                                      .setMessage(message)
                                      .build());
    }

    public void markConsumed() {
        if (completed.compareAndSet(false, true)) {
            onConsumed.run();
        }
    }
}
