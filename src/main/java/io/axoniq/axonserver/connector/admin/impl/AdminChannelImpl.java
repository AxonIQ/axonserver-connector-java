package io.axoniq.axonserver.connector.admin.impl;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.CompletableFutureStreamObserver;
import io.axoniq.axonserver.grpc.admin.EventProcessorAdminServiceGrpc;
import io.axoniq.axonserver.grpc.admin.EventProcessorAdminServiceGrpc.EventProcessorAdminServiceStub;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;
import io.axoniq.axonserver.grpc.control.ClientIdentification;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nonnull;

/**
 * {@link AdminChannel} GRPC implementation to allow a client application sending
 * and receiving administration related messages to and from Axon Server.
 *
 * @author Sara Pellegrini
 * @since 4.6
 */
public class AdminChannelImpl extends AbstractAxonServerChannel<Void> implements AdminChannel {

    private final EventProcessorAdminServiceStub eventProcessorServiceStub;

    public AdminChannelImpl(ClientIdentification clientIdentification,
                            ScheduledExecutorService executor, AxonServerManagedChannel channel) {
        super(clientIdentification, executor, channel);
        eventProcessorServiceStub = EventProcessorAdminServiceGrpc.newStub(channel);
    }

    @Override
    public CompletableFuture<Void> pauseEventProcessor(String eventProcessorName, String tokenStoreIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        CompletableFuture<Void> response = new CompletableFuture<>();
        CompletableFutureStreamObserver<Empty, Void> responseObserver = new CompletableFutureStreamObserver<>(response);
        eventProcessorServiceStub.pauseEventProcessor(eventProcessorIdentifier, responseObserver);
        return response;
    }

    @Override
    public CompletableFuture<Void> startEventProcessor(String eventProcessorName, String tokenStoreIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        CompletableFuture<Void> response = new CompletableFuture<>();
        CompletableFutureStreamObserver<Empty, Void> responseObserver = new CompletableFutureStreamObserver<>(response);
        eventProcessorServiceStub.startEventProcessor(eventProcessorIdentifier, responseObserver);
        return response;
    }

    @Nonnull
    private EventProcessorIdentifier eventProcessorId(String eventProcessorName, String tokenStoreIdentifier) {
        return EventProcessorIdentifier.newBuilder()
                                       .setProcessorName(eventProcessorName)
                                       .setTokenStoreIdentifier(tokenStoreIdentifier)
                                       .build();
    }

    @Override
    public CompletableFuture<Void> splitEventProcessor(String eventProcessorName, String tokenStoreIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        CompletableFuture<Void> response = new CompletableFuture<>();
        CompletableFutureStreamObserver<Empty, Void> responseObserver = new CompletableFutureStreamObserver<>(response);
        eventProcessorServiceStub.splitEventProcessor(eventProcessorIdentifier, responseObserver);
        return response;
    }

    @Override
    public CompletableFuture<Void> mergeEventProcessor(String eventProcessorName, String tokenStoreIdentifier) {
        EventProcessorIdentifier eventProcessorIdentifier = eventProcessorId(eventProcessorName, tokenStoreIdentifier);
        CompletableFuture<Void> response = new CompletableFuture<>();
        CompletableFutureStreamObserver<Empty, Void> responseObserver = new CompletableFutureStreamObserver<>(response);
        eventProcessorServiceStub.mergeEventProcessor(eventProcessorIdentifier, responseObserver);
        return response;
    }

    @Override
    public void connect() {
        // there is no stream for the admin channel (yet)
    }

    @Override
    public void reconnect() {
        // there is no stream for the admin channel (yet)
    }

    @Override
    public void disconnect() {
        // there is no stream for the admin channel (yet)
    }

    @Override
    public boolean isReady() {
        return true;
    }
}
