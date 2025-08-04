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

/**
 * Communication channel for Snapshot-related interactions with AxonServer. This interface provides operations for
 * managing snapshots in a Dynamic Consistency Boundaries (DCB) environment, including adding, deleting, listing, and
 * retrieving snapshots.
 */
public interface SnapshotChannel {

    /**
     * Adds a snapshot to the store. If a snapshot with the same key and sequence already exists, it will be replaced.
     * When the prune flag is set to true, all snapshots with the same key and a sequence number less than the provided
     * sequence will be removed.
     *
     * @param request The request containing the snapshot to add, including key, sequence, prune flag, and snapshot
     *                data
     * @return A CompletableFuture that resolves to an AddSnapshotResponse when the operation completes
     */
    CompletableFuture<AddSnapshotResponse> addSnapshot(AddSnapshotRequest request);

    /**
     * Deletes snapshots within a specified sequence range for a given key. The range is inclusive at the lower bound
     * (fromSequence) and exclusive at the upper bound (toSequence).
     *
     * @param request The request containing the key and sequence range to delete
     * @return A CompletableFuture that resolves to a DeleteSnapshotsResponse when the operation completes
     */
    CompletableFuture<DeleteSnapshotsResponse> deleteSnapshots(DeleteSnapshotsRequest request);

    /**
     * Lists snapshots for a given key within a specified sequence range. The range is inclusive at the lower bound
     * (fromSequence) and exclusive at the upper bound (toSequence). Snapshots are returned in ascending order by
     * sequence number.
     *
     * @param request The request containing the key and sequence range to list
     * @return A ResultStream of ListSnapshotsResponse objects containing the matching snapshots
     */
    ResultStream<ListSnapshotsResponse> listSnapshots(ListSnapshotsRequest request);

    /**
     * Retrieves the snapshot with the highest sequence number for a given key. If no snapshots exist for the key, the
     * response will not contain a snapshot.
     *
     * @param request The request containing the key to get the last snapshot for
     * @return A CompletableFuture that resolves to a GetLastSnapshotResponse when the operation completes
     */
    CompletableFuture<GetLastSnapshotResponse> getLastSnapshot(GetLastSnapshotRequest request);
}
