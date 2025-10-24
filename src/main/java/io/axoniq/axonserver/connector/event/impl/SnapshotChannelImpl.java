package io.axoniq.axonserver.connector.event.impl;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.event.SnapshotChannel;
import io.axoniq.axonserver.connector.impl.AbstractAxonServerChannel;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.connector.impl.AxonServerManagedChannel;
import io.axoniq.axonserver.connector.impl.FutureStreamObserver;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.event.dcb.AddSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.AddSnapshotResponse;
import io.axoniq.axonserver.grpc.event.dcb.DcbSnapshotStoreGrpc;
import io.axoniq.axonserver.grpc.event.dcb.DeleteSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.dcb.DeleteSnapshotsResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetLastSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetLastSnapshotResponse;
import io.axoniq.axonserver.grpc.event.dcb.ListSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.dcb.ListSnapshotsResponse;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link SnapshotChannel} implementation, serving as the event connection between Axon Server and a client
 * application.
 *
 * @author Silvano Biemans
 * @author Milan Savic
 * @since 2025.2.0
 */
public class SnapshotChannelImpl extends AbstractAxonServerChannel<Void> implements SnapshotChannel {

    private final DcbSnapshotStoreGrpc.DcbSnapshotStoreStub snapshotStore;
    private final ClientIdentification clientIdentification;
    private final Set<ResultStream<ListSnapshotsResponse>> buffers = ConcurrentHashMap.newKeySet();

    private static final int BUFFER_SIZE = 512;
    private static final int REFILL_BATCH = 16;

    /**
     * Instantiate an {@link AbstractAxonServerChannel}.
     *
     * @param clientIdentification     identifies a client
     * @param executor                 a {@link ScheduledExecutorService} used to schedule reconnections
     * @param axonServerManagedChannel the {@link AxonServerManagedChannel} used to connect to AxonServer
     */
    public SnapshotChannelImpl(ClientIdentification clientIdentification, ScheduledExecutorService executor,
                               AxonServerManagedChannel axonServerManagedChannel) {
        super(clientIdentification, executor, axonServerManagedChannel);
        this.snapshotStore = DcbSnapshotStoreGrpc.newStub(axonServerManagedChannel);
        this.clientIdentification = clientIdentification;
    }

    @Override
    public CompletableFuture<AddSnapshotResponse> addSnapshot(AddSnapshotRequest request) {
        FutureStreamObserver<AddSnapshotResponse> res = new FutureStreamObserver<>(null);
        this.snapshotStore.add(request, res);
        return res;
    }

    @Override
    public CompletableFuture<DeleteSnapshotsResponse> deleteSnapshots(DeleteSnapshotsRequest request) {
        FutureStreamObserver<DeleteSnapshotsResponse> res = new FutureStreamObserver<>(null);
        this.snapshotStore.delete(request, res);
        return res;
    }

    @Override
    public ResultStream<ListSnapshotsResponse> listSnapshots(ListSnapshotsRequest request) {
        AbstractBufferedStream<ListSnapshotsResponse, Empty> buffer =
                new AbstractBufferedStream<>(clientIdentification.getClientId(),
                                             BUFFER_SIZE,
                                             REFILL_BATCH) {
                    @Override
                    protected ListSnapshotsResponse terminalMessage() {
                        return ListSnapshotsResponse.newBuilder()
                                                    .build();
                    }

                    @Override
                    protected Empty buildFlowControlMessage(FlowControl flowControl) {
                        return null;
                    }
                };
        buffers.add(buffer);
        buffer.onCloseRequested(() -> buffers.remove(buffer));
        try {
            snapshotStore.list(request, buffer);
        } catch (Exception e) {
            buffers.remove(buffer);
            throw e;
        }
        return buffer;
    }

    @Override
    public CompletableFuture<GetLastSnapshotResponse> getLastSnapshot(GetLastSnapshotRequest request) {
        FutureStreamObserver<GetLastSnapshotResponse> res = new FutureStreamObserver<>(null);
        this.snapshotStore.getLast(request, res);
        return res;
    }

    @Override
    public void connect() {
        // nothing to do here
    }

    @Override
    public void reconnect() {
        closeBuffers();
    }

    @Override
    public void disconnect() {
        closeBuffers();
    }

    @Override
    public boolean isReady() {
        return true;
    }

    private void closeBuffers() {
        buffers.forEach(ResultStream::close);
        buffers.clear();
    }
}
