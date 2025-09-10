package io.axoniq.axonserver.connector.event.dcb;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStreamPublisher;
import io.axoniq.axonserver.connector.event.SnapshotChannel;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.grpc.event.dcb.AddSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.AddSnapshotResponse;
import io.axoniq.axonserver.grpc.event.dcb.DeleteSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetLastSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetLastSnapshotResponse;
import io.axoniq.axonserver.grpc.event.dcb.ListSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.dcb.ListSnapshotsResponse;
import io.axoniq.axonserver.grpc.event.dcb.Snapshot;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;

class DcbSnapshotChannelTest extends AbstractAxonServerIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(DcbSnapshotChannelTest.class);
    private static final boolean LOCAL = false;

    private AxonServerConnection connection;
    private AxonServerConnectionFactory client;
    private SnapshotChannel snapshotChannel;

    @BeforeEach
    void setUp() {
        AxonServerConnectionFactory.Builder builder = AxonServerConnectionFactory.forClient("dcb-snapshot-test")
                                                                                 .connectTimeout(1500,
                                                                                                 TimeUnit.MILLISECONDS)
                                                                                 .reconnectInterval(500, MILLISECONDS);

        if (LOCAL) {
            builder.routingServers(new ServerAddress("localhost", 8124));
            logger.info("Using local Axon Server instance at localhost:8124");
        } else {
            builder.routingServers(axonServerAddress);
        }

        client = builder.build();
        connection = client.connect("default");
        snapshotChannel = connection.snapshotChannel();
    }

    @AfterEach
    void tearDown() {
        client.shutdown();
    }

    @Override
    protected boolean dcbContext() {
        return true;
    }

    // Basic Put/Overwrite Semantics Tests

    @Test
    void addSnapshotWithSameSequenceReplacesExisting() {
        ByteString key = ByteString.copyFrom("replace-test".getBytes());
        ByteString value1 = ByteString.copyFrom("first".getBytes());
        ByteString value2 = ByteString.copyFrom("second".getBytes());
        long sequence = 1L;

        // Add first snapshot
        AddSnapshotRequest request1 = AddSnapshotRequest.newBuilder()
                                                        .setKey(key)
                                                        .setSequence(sequence)
                                                        .setPrune(false)
                                                        .setSnapshot(Snapshot.newBuilder()
                                                                             .setName("snapshot1")
                                                                             .setVersion("1.0")
                                                                             .setPayload(value1)
                                                                             .build())
                                                        .build();

        snapshotChannel.addSnapshot(request1).join();

        // Add second snapshot with same sequence
        AddSnapshotRequest request2 = AddSnapshotRequest.newBuilder()
                                                        .setKey(key)
                                                        .setSequence(sequence)
                                                        .setPrune(false)
                                                        .setSnapshot(Snapshot.newBuilder()
                                                                             .setName("snapshot2")
                                                                             .setVersion("1.0")
                                                                             .setPayload(value2)
                                                                             .build())
                                                        .build();

        snapshotChannel.addSnapshot(request2).join();

        // Verify only second snapshot exists
        GetLastSnapshotResponse last = snapshotChannel.getLastSnapshot(
                GetLastSnapshotRequest.newBuilder().setKey(key).build()
        ).join();

        assertNotNull(last);
        assertEquals(sequence, last.getSequence());
        assertEquals(value2, last.getSnapshot().getPayload());

        // Verify through list
        List<ListSnapshotsResponse> snapshots = collectSnapshots(key, 0, 2);
        assertEquals(1, snapshots.size());
        assertEquals(value2, snapshots.get(0).getSnapshot().getPayload());
    }

    @Test
    void addNewSnapshotToEmptyStoreIsRetrievable() {
        ByteString key = ByteString.copyFrom("new-snapshot".getBytes());
        ByteString value = ByteString.copyFrom("test-value".getBytes());
        long sequence = 42L;

        AddSnapshotRequest request = AddSnapshotRequest.newBuilder()
                                                       .setKey(key)
                                                       .setSequence(sequence)
                                                       .setPrune(false)
                                                       .setSnapshot(Snapshot.newBuilder()
                                                                            .setName("test-snapshot")
                                                                            .setVersion("1.0")
                                                                            .setTimestamp(System.currentTimeMillis())
                                                                            .setPayload(value)
                                                                            .build())
                                                       .build();

        snapshotChannel.addSnapshot(request).join();

        GetLastSnapshotResponse result = snapshotChannel.getLastSnapshot(
                GetLastSnapshotRequest.newBuilder().setKey(key).build()
        ).join();

        assertNotNull(result);
        assertEquals(sequence, result.getSequence());
        assertEquals(value, result.getSnapshot().getPayload());
    }

    // Pruning Behavior Tests

    @Test
    void addingSnapshotWithPruneEnabledKeepsOnlySnapshotsGreaterOrEqualToAddedSequence() {
        ByteString key = ByteString.copyFrom("prune-test".getBytes());
        ByteString value = ByteString.copyFrom("data".getBytes());

        // Add snapshots at sequences 50, 100, 150
        addSnapshot(key, 50L, false, value);
        addSnapshot(key, 100L, false, value);
        addSnapshot(key, 150L, false, value);
        System.out.println("Got here");
        // Add at sequence 75 with pruning - should keep 75, 100, 150
        addSnapshot(key, 75L, true, value);

        List<ListSnapshotsResponse> snapshots = collectSnapshots(key, 0, 200);
        List<Long> sequences = snapshots.stream()
                                        .map(ListSnapshotsResponse::getSequence)
                                        .sorted()
                                        .collect(Collectors.toList());

        assertEquals(List.of(75L, 100L, 150L), sequences);
    }

    @Test
    void pruningOnlyAffectsSnapshotsWithSameKey() {
        ByteString key1 = ByteString.copyFrom("key1".getBytes());
        ByteString key2 = ByteString.copyFrom("key2".getBytes());
        ByteString value = ByteString.copyFrom("data".getBytes());

        // Add snapshots to both keys
        addSnapshot(key1, 50L, false, value);
        addSnapshot(key1, 100L, false, value);
        addSnapshot(key2, 25L, false, value);
        addSnapshot(key2, 75L, false, value);

        // Prune key1 at sequence 80
        addSnapshot(key1, 80L, true, value);

        // Check key1 - should have 80 and 100
        List<ListSnapshotsResponse> snapshots1 = collectSnapshots(key1, 0, 200);
        List<Long> sequences1 = snapshots1.stream()
                                          .map(ListSnapshotsResponse::getSequence)
                                          .sorted()
                                          .collect(Collectors.toList());
        assertEquals(List.of(80L, 100L), sequences1);

        // Check key2 - should be unchanged
        List<ListSnapshotsResponse> snapshots2 = collectSnapshots(key2, 0, 200);
        List<Long> sequences2 = snapshots2.stream()
                                          .map(ListSnapshotsResponse::getSequence)
                                          .sorted()
                                          .collect(Collectors.toList());
        assertEquals(List.of(25L, 75L), sequences2);
    }

    // Non-Pruning Behavior Tests

    @Test
    void addingSubsequentSnapshotsWithPruneDisabledKeepsAllSnapshots() throws InterruptedException {
        ByteString key = ByteString.copyFrom("no-prune".getBytes());
        ByteString value = ByteString.copyFrom("world".getBytes());
        int limit = 3;
        CountDownLatch latch = new CountDownLatch(limit);

        for (long i = 0; i < limit; i++) {
            long seq = i;
            CompletableFuture<AddSnapshotResponse> future = snapshotChannel.addSnapshot(
                    AddSnapshotRequest.newBuilder()
                                      .setKey(key)
                                      .setSequence(seq)
                                      .setPrune(false)
                                      .setSnapshot(Snapshot.newBuilder()
                                                           .setName("snapshot-" + seq)
                                                           .setVersion("1.0")
                                                           .setPayload(value)
                                                           .build())
                                      .build()
            );
            future.thenRun(latch::countDown);
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        GetLastSnapshotResponse last = snapshotChannel.getLastSnapshot(
                GetLastSnapshotRequest.newBuilder().setKey(key).build()
        ).join();

        assertNotNull(last);
        assertEquals(limit - 1, last.getSequence());

        List<ListSnapshotsResponse> snapshots = collectSnapshots(key, 0, limit);
        assertEquals(limit, snapshots.size());
    }

    // Delete Operation Tests

    @Test
    void deleteRangeRemovesSnapshotsWithinExclusiveBounds() {
        ByteString key = ByteString.copyFrom("delete-range".getBytes());
        ByteString value = ByteString.copyFrom("data".getBytes());

        // Add snapshots at 50, 100, 150, 200
        List<Long> sequences = List.of(50L, 100L, 150L, 200L);
        sequences.forEach(seq -> addSnapshot(key, seq, false, value));

        // Delete from 100 to 200 (should delete 100 and 150, not 200)
        snapshotChannel.deleteSnapshots(
                DeleteSnapshotsRequest.newBuilder()
                                      .setKey(key)
                                      .setToSequence(200)
                                      .build()
        ).join();

        List<ListSnapshotsResponse> snapshots = collectSnapshots(key, 0, 250);
        List<Long> remainingSequences = snapshots.stream()
                                                 .map(ListSnapshotsResponse::getSequence)
                                                 .sorted()
                                                 .collect(Collectors.toList());

        assertEquals(List.of(200L), remainingSequences);
    }

    @Test
    void deletingAllSnapshotsMakesGetLastThrowException() {
        ByteString key = ByteString.copyFrom("delete-all".getBytes());
        ByteString value = ByteString.copyFrom("data".getBytes());

        // Add some snapshots
        addSnapshot(key, 50L, false, value);
        addSnapshot(key, 100L, false, value);

        // Delete all
        snapshotChannel.deleteSnapshots(
                DeleteSnapshotsRequest.newBuilder()
                                      .setKey(key)
                                      .setToSequence(1000)
                                      .build()
        ).join();

        // GetLast should return empty response
        assertThrows(CompletionException.class, ()->{
            snapshotChannel.getLastSnapshot(
                    GetLastSnapshotRequest.newBuilder().setKey(key).build()
            ).join();
        });

    }

    @Test
    void sequenceBecomesImmediatelyReusableAfterDeletion() {
        ByteString key = ByteString.copyFrom("reuse-sequence".getBytes());
        ByteString value1 = ByteString.copyFrom("original".getBytes());
        ByteString value2 = ByteString.copyFrom("reused".getBytes());
        long sequence = 100L;

        // Add snapshot
        addSnapshot(key, sequence, false, value1);

        // Verify it exists
        GetLastSnapshotResponse first = snapshotChannel.getLastSnapshot(
                GetLastSnapshotRequest.newBuilder().setKey(key).build()
        ).join();
        assertEquals(value1, first.getSnapshot().getPayload());

        // Delete it
        snapshotChannel.deleteSnapshots(
                DeleteSnapshotsRequest.newBuilder()
                                      .setKey(key)
                                      .setToSequence(sequence + 1)
                                      .build()
        ).join();

        // Verify it's gone
        assertThrows(CompletionException.class, ()->{
            snapshotChannel.getLastSnapshot(
                    GetLastSnapshotRequest.newBuilder().setKey(key).build()
            ).join();
        });

        // Reuse the same sequence
        addSnapshot(key, sequence, false, value2);

        // Verify new snapshot is there
        GetLastSnapshotResponse reused = snapshotChannel.getLastSnapshot(
                GetLastSnapshotRequest.newBuilder().setKey(key).build()
        ).join();
        assertTrue(reused.hasSnapshot());
        assertEquals(sequence, reused.getSequence());
        assertEquals(value2, reused.getSnapshot().getPayload());
    }

    // List Operation Tests

    @Test
    void listReturnsEmptyWhenNoSnapshotsExistInRange() {
        ByteString key = ByteString.copyFrom("empty-range".getBytes());

        List<ListSnapshotsResponse> snapshots = collectSnapshots(key, 100, 200);
        assertEquals(0, snapshots.size());
    }

    @Test
    void listReturnsSnapshotsOrderedBySequence() {
        ByteString key = ByteString.copyFrom("order-test".getBytes());
        ByteString value = ByteString.copyFrom("val".getBytes());

        // Add in random order
        List<Long> sequences = List.of(150L, 50L, 200L, 100L, 75L);
        sequences.forEach(seq -> addSnapshot(key, seq, false, value));

        List<ListSnapshotsResponse> snapshots = collectSnapshots(key, 0, 300);
        List<Long> returnedSequences = snapshots.stream()
                                                .map(ListSnapshotsResponse::getSequence)
                                                .collect(Collectors.toList());

        assertEquals(returnedSequences.stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList()), returnedSequences);
    }

    @Test
    void listRespectsInclusiveLowerAndExclusiveUpperBounds() {
        ByteString key = ByteString.copyFrom("bounds-test".getBytes());
        ByteString value = ByteString.copyFrom("data".getBytes());

        // Add snapshots at boundaries and beyond
        List<Long> sequences = List.of(25L, 50L, 75L, 100L, 125L);
        sequences.forEach(seq -> addSnapshot(key, seq, false, value));

        List<ListSnapshotsResponse> snapshots = collectSnapshots(key, 50, 100);
        List<Long> returnedSequences = snapshots.stream()
                                                .map(ListSnapshotsResponse::getSequence)
                                                .sorted()
                                                .collect(Collectors.toList());

        assertEquals(List.of(50L, 75L), returnedSequences);
    }

    // GetLast Operation Tests

    @Test
    void getLastReturnsNullWhenNoSnapshotExistsForKey() {
        ByteString key = ByteString.copyFrom("unknown-key".getBytes());

        assertThrows(CompletionException.class, ()->{
            snapshotChannel.getLastSnapshot(
                    GetLastSnapshotRequest.newBuilder().setKey(key).build()
            ).join();
        });
    }

    // Complex Scenarios

    @Test
    void switchingFromNonPruningToPruningStrategyCleansUpOldSnapshots() {
        ByteString key = ByteString.copyFrom("strategy-switch".getBytes());
        ByteString value = ByteString.copyFrom("data".getBytes());

        // Accumulate many snapshots without pruning
        LongStream.range(0, 100).forEach(seq -> addSnapshot(key, seq, false, value));

        // Verify all exist
        List<ListSnapshotsResponse> beforeSnapshots = collectSnapshots(key, 0, 100);
        assertEquals(100, beforeSnapshots.size());

        // Switch to pruning strategy
        addSnapshot(key, 95L, true, value);

        // Verify cleanup
        List<ListSnapshotsResponse> afterSnapshots = collectSnapshots(key, 0, 100);
        assertEquals(5, afterSnapshots.size()); // 95, 96, 97, 98, 99

        long minSequence = afterSnapshots.stream()
                                         .mapToLong(ListSnapshotsResponse::getSequence)
                                         .min()
                                         .orElse(-1);
        assertEquals(95L, minSequence);
    }

    @Test
    void addDeleteAddSameSequenceDemonstratesSequenceReusability() {
        ByteString key = ByteString.copyFrom("add-delete-add".getBytes());
        ByteString value1 = ByteString.copyFrom("first".getBytes());
        ByteString value2 = ByteString.copyFrom("second".getBytes());
        long sequence = 50L;

        // Add
        addSnapshot(key, sequence, false, value1);

        // Verify
        GetLastSnapshotResponse first = snapshotChannel.getLastSnapshot(
                GetLastSnapshotRequest.newBuilder().setKey(key).build()
        ).join();
        assertEquals(value1, first.getSnapshot().getPayload());

        // Delete
        snapshotChannel.deleteSnapshots(
                DeleteSnapshotsRequest.newBuilder()
                                      .setKey(key)
                                      .setToSequence(sequence + 1)
                                      .build()
        ).join();

        // Verify deletion
        assertThrows(CompletionException.class, ()->{
            snapshotChannel.getLastSnapshot(
                    GetLastSnapshotRequest.newBuilder().setKey(key).build()
            ).join();
        });

        // Add again with same sequence
        addSnapshot(key, sequence, false, value2);

        // Verify new value
        GetLastSnapshotResponse second = snapshotChannel.getLastSnapshot(
                GetLastSnapshotRequest.newBuilder().setKey(key).build()
        ).join();
        assertTrue(second.hasSnapshot());
        assertEquals(sequence, second.getSequence());
        assertEquals(value2, second.getSnapshot().getPayload());
    }

    // Performance Tests (disabled by default)

    @Test
//    @Disabled("Performance test - enable manually")
    void appendingTwoMillionSnapshotsWithPruneEnabledCompletesInReasonableTime() throws InterruptedException {
        ByteString key = ByteString.copyFrom("perf-prune".getBytes());
        ByteString value = ByteString.copyFrom("payload".getBytes());
        long totalSnapshots = 2_000_000L;

        long startTime = System.currentTimeMillis();

        List<CompletableFuture<AddSnapshotResponse>> futures = new ArrayList<>();
        for (long i = 0; i < totalSnapshots; i++) {
            if (i > 0 && i % 1_000_000 == 0) {
                logger.info("Written {} snapshots...", i);
            }
            futures.add(snapshotChannel.addSnapshot(
                    AddSnapshotRequest.newBuilder()
                                      .setKey(key)
                                      .setSequence(i)
                                      .setPrune(true)
                                      .setSnapshot(Snapshot.newBuilder()
                                                           .setName("snapshot")
                                                           .setVersion("1.0")
                                                           .setPayload(value)
                                                           .build())
                                      .build()
            ));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long duration = (System.currentTimeMillis() - startTime) / 1000;
        logger.info("Completed 2M writes with prune in {} seconds", duration);

        // Verify only last snapshot remains
        GetLastSnapshotResponse last = snapshotChannel.getLastSnapshot(
                GetLastSnapshotRequest.newBuilder().setKey(key).build()
        ).join();
        assertTrue(last.hasSnapshot());
        assertEquals(totalSnapshots - 1, last.getSequence());

        List<ListSnapshotsResponse> snapshots = collectSnapshots(key, 0, totalSnapshots);
        assertEquals(1, snapshots.size());
    }

    // Helper methods

    private void addSnapshot(ByteString key, long sequence, boolean prune, ByteString value) {
        snapshotChannel.addSnapshot(
                AddSnapshotRequest.newBuilder()
                                  .setKey(key)
                                  .setSequence(sequence)
                                  .setPrune(prune)
                                  .setSnapshot(Snapshot.newBuilder()
                                                       .setName("snapshot-" + sequence)
                                                       .setVersion("1.0")
                                                       .setPayload(value)
                                                       .build())
                                  .build()
        ).join();
    }

    private List<ListSnapshotsResponse> collectSnapshots(ByteString key, long fromSequence, long toSequence) {
        ListSnapshotsRequest request = ListSnapshotsRequest.newBuilder()
                                                           .setKey(key)
                                                           .setFromSequence(fromSequence)
                                                           .setToSequence(toSequence)
                                                           .build();

        AtomicReference<List<ListSnapshotsResponse>> result = new AtomicReference<>(new ArrayList<>());

        StepVerifier.create(Flux.from(new ResultStreamPublisher<>(() -> snapshotChannel.listSnapshots(request))))
                    .recordWith(() -> result.get())
                    .thenConsumeWhile(response -> response.hasSnapshot())
                    .verifyComplete();

        return result.get();
    }
}