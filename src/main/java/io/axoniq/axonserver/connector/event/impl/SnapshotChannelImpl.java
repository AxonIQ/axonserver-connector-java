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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class SnapshotChannelImpl extends AbstractAxonServerChannel<Void> implements SnapshotChannel {

    private final DcbSnapshotStoreGrpc.DcbSnapshotStoreStub snapshotStore;
    private final ClientIdentification clientIdentification;

    private static final int BUFFER_SIZE = 512;
    private static final int REFILL_BATCH = 16;
    /**
     * Instantiate an {@link AbstractAxonServerChannel}.
     *
     * @param clientIdentification identifies a client
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
        AbstractBufferedStream<ListSnapshotsResponse, Empty> result =
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
        snapshotStore.list(request, result);
        return result;
    }

    @Override
    public CompletableFuture<GetLastSnapshotResponse> getLastSnapshot(GetLastSnapshotRequest request) {
        FutureStreamObserver<GetLastSnapshotResponse> res = new FutureStreamObserver<>(null);
        this.snapshotStore.getLast(request, res);
        return res;
    }

    @Override
    public void connect() {

    }

    @Override
    public void reconnect() {

    }

    @Override
    public void disconnect() {

    }

    @Override
    public boolean isReady() {
        return false;
    }
}
