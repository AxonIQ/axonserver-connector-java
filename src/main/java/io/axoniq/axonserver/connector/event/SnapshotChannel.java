package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.dcb.AddSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.AddSnapshotResponse;
import io.axoniq.axonserver.grpc.event.dcb.DeleteSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.dcb.DeleteSnapshotsResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetLastSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetLastSnapshotResponse;
import io.axoniq.axonserver.grpc.event.dcb.ListSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.dcb.ListSnapshotsResponse;

import java.util.concurrent.CompletableFuture;

public interface SnapshotChannel {
    CompletableFuture<AddSnapshotResponse> addSnapshot(AddSnapshotRequest request);
    CompletableFuture<DeleteSnapshotsResponse> deleteSnapshots(DeleteSnapshotsRequest request);
    ResultStream<ListSnapshotsResponse> listSnapshots(ListSnapshotsRequest request);
    CompletableFuture<GetLastSnapshotResponse> getLastSnapshot(GetLastSnapshotRequest request);
}
