/*
 * Copyright (c) 2020-2026. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.event.dcb;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.ResultStreamPublisher;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.connector.event.SnapshotChannel;
import io.axoniq.axonserver.connector.event.SnapshottedDcbEventChannel;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.grpc.event.dcb.AddSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.Criterion;
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.ListSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.dcb.ListSnapshotsResponse;
import io.axoniq.axonserver.grpc.event.dcb.Snapshot;
import io.axoniq.axonserver.grpc.event.dcb.SnapshottedSourceEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.SnapshottedSourceRequest;
import io.axoniq.axonserver.grpc.event.dcb.Tag;
import io.axoniq.axonserver.grpc.event.dcb.TaggedEvent;
import io.axoniq.axonserver.grpc.event.dcb.TagsAndNamesCriterion;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.fail;

class SnapshottedDcbEventChannelTest extends AbstractAxonServerIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(SnapshottedDcbEventChannelTest.class);
    private static final boolean LOCAL = false;

    private AxonServerConnection connection;
    private AxonServerConnectionFactory client;
    private DcbEventChannel dcbEventChannel;
    private SnapshotChannel snapshotChannel;
    private SnapshottedDcbEventChannel snapshottedDcbEventChannel;

    @BeforeEach
    void setUp() {
        AxonServerConnectionFactory.Builder builder =
                AxonServerConnectionFactory.forClient("snapshotted-dcb-test")
                                           .connectTimeout(1500, TimeUnit.MILLISECONDS)
                                           .reconnectInterval(500, MILLISECONDS);

        if (LOCAL) {
            builder.routingServers(new ServerAddress("localhost", 8124));
            logger.info("Using local Axon Server instance at localhost:8124");
        } else {
            builder.routingServers(axonServerAddress);
        }

        client = builder.build();
        connection = client.connect("default");
        dcbEventChannel = connection.dcbEventChannel();
        snapshotChannel = connection.snapshotChannel();
        snapshottedDcbEventChannel = connection.snapshottedDcbEventChannel();

        assumeSnapshotSupportOnAxonServer();
    }

    @AfterEach
    void tearDown() {
        client.shutdown();
    }

    @Override
    protected boolean dcbContext() {
        return true;
    }

    private void assumeSnapshotSupportOnAxonServer() {
        try (ResultStream<ListSnapshotsResponse> probe =
                     snapshotChannel.listSnapshots(ListSnapshotsRequest.newBuilder().build())) {
            probe.nextIfAvailable(2, SECONDS);
            Assumptions.assumeFalse(probe.getError().isPresent(),
                                    "AxonServer does not support snapshot channel");
        } catch (StatusRuntimeException e) {
            Assumptions.assumeFalse(e.getStatus() == Status.UNIMPLEMENTED,
                                    "AxonServer does not support snapshot channel");
        } catch (InterruptedException e) {
            fail(e);
        }
    }

    @Test
    void sourceWithoutSnapshotEmitsOnlyMatchingEventsAndConsistencyMarker() {
        Tag tag = aTag();
        String eventName = aString();
        ByteString snapshotKey = ByteString.copyFromUtf8("no-snapshot-" + UUID.randomUUID());

        long head = head();

        Event matching1 = anEvent(aString(), eventName);
        Event matching2 = anEvent(aString(), eventName);
        appendEvent(taggedEvent(matching1, tag));
        appendEvent(taggedEvent(anEvent(aString(), eventName)));   // unmatched (no tag)
        appendEvent(taggedEvent(matching2, tag));

        SnapshottedSourceRequest request = SnapshottedSourceRequest.newBuilder()
                                                                   .setSnapshotKey(snapshotKey)
                                                                   .addCriterion(tagAndNameCriterion(tag, eventName))
                                                                   .build();

        StepVerifier.create(new ResultStreamPublisher<>(() -> snapshottedDcbEventChannel.source(request)))
                    .expectNextMatches(r -> r.getResultCase() == SnapshottedSourceEventsResponse.ResultCase.EVENT
                            && r.getEvent().getSequence() == head
                            && r.getEvent().getEvent().equals(matching1))
                    .expectNextMatches(r -> r.getResultCase() == SnapshottedSourceEventsResponse.ResultCase.EVENT
                            && r.getEvent().getSequence() == head + 2
                            && r.getEvent().getEvent().equals(matching2))
                    .expectNextMatches(r -> r.getResultCase()
                            == SnapshottedSourceEventsResponse.ResultCase.CONSISTENCY_MARKER
                            && r.getConsistencyMarker() == head + 3)
                    .verifyComplete();
    }

    @Test
    void sourceWithSnapshotEmitsSnapshotFirstThenPostSnapshotEvents() {
        Tag tag = aTag();
        String eventName = aString();
        ByteString snapshotKey = ByteString.copyFromUtf8("snapshotted-" + UUID.randomUUID());
        ByteString snapshotPayload = ByteString.copyFromUtf8("payload-" + UUID.randomUUID());

        long head = head();

        // pre-snapshot events: filtered out by the snapshot's sequence
        Event preEvent1 = anEvent(aString(), eventName);
        Event preEvent2 = anEvent(aString(), eventName);
        appendEvent(taggedEvent(preEvent1, tag));
        appendEvent(taggedEvent(preEvent2, tag));
        long snapshotSequence = head + 1;

        addSnapshot(snapshotKey, snapshotSequence, snapshotPayload);

        // post-snapshot events
        Event postEvent1 = anEvent(aString(), eventName);
        Event postEvent2 = anEvent(aString(), eventName);
        appendEvent(taggedEvent(postEvent1, tag));
        appendEvent(taggedEvent(postEvent2, tag));

        SnapshottedSourceRequest request = SnapshottedSourceRequest.newBuilder()
                                                                   .setSnapshotKey(snapshotKey)
                                                                   .addCriterion(tagAndNameCriterion(tag, eventName))
                                                                   .build();

        StepVerifier.create(new ResultStreamPublisher<>(() -> snapshottedDcbEventChannel.source(request)))
                    .expectNextMatches(r -> r.getResultCase() == SnapshottedSourceEventsResponse.ResultCase.SNAPSHOT
                            && r.getSnapshot().getPayload().equals(snapshotPayload))
                    .expectNextMatches(r -> r.getResultCase() == SnapshottedSourceEventsResponse.ResultCase.EVENT
                            && r.getEvent().getSequence() == head + 2
                            && r.getEvent().getEvent().equals(postEvent1))
                    .expectNextMatches(r -> r.getResultCase() == SnapshottedSourceEventsResponse.ResultCase.EVENT
                            && r.getEvent().getSequence() == head + 3
                            && r.getEvent().getEvent().equals(postEvent2))
                    .expectNextMatches(r -> r.getResultCase()
                            == SnapshottedSourceEventsResponse.ResultCase.CONSISTENCY_MARKER
                            && r.getConsistencyMarker() == head + 4)
                    .verifyComplete();
    }

    @Test
    void sourceWithSnapshotAndNoMatchingPostSnapshotEventsEmitsSnapshotAndConsistencyMarker() {
        Tag tag = aTag();
        String eventName = aString();
        ByteString snapshotKey = ByteString.copyFromUtf8("only-snapshot-" + UUID.randomUUID());
        ByteString snapshotPayload = ByteString.copyFromUtf8("payload-" + UUID.randomUUID());

        long head = head();

        appendEvent(taggedEvent(anEvent(aString(), eventName), tag));
        long snapshotSequence = head;

        addSnapshot(snapshotKey, snapshotSequence, snapshotPayload);

        // unrelated event after the snapshot
        appendEvent(taggedEvent(anEvent(aString(), aString())));

        SnapshottedSourceRequest request = SnapshottedSourceRequest.newBuilder()
                                                                   .setSnapshotKey(snapshotKey)
                                                                   .addCriterion(tagAndNameCriterion(tag, eventName))
                                                                   .build();

        StepVerifier.create(new ResultStreamPublisher<>(() -> snapshottedDcbEventChannel.source(request)))
                    .expectNextMatches(r -> r.getResultCase() == SnapshottedSourceEventsResponse.ResultCase.SNAPSHOT
                            && r.getSnapshot().getPayload().equals(snapshotPayload))
                    .expectNextMatches(r -> r.getResultCase()
                            == SnapshottedSourceEventsResponse.ResultCase.CONSISTENCY_MARKER
                            && r.getConsistencyMarker() == head + 2)
                    .verifyComplete();
    }

    @Test
    void sourceWithUnknownSnapshotKeyDoesNotEmitSnapshot() {
        Tag tag = aTag();
        String eventName = aString();
        ByteString snapshotKey = ByteString.copyFromUtf8("known-" + UUID.randomUUID());
        ByteString unknownSnapshotKey = ByteString.copyFromUtf8("unknown-" + UUID.randomUUID());
        ByteString snapshotPayload = ByteString.copyFromUtf8("payload-" + UUID.randomUUID());

        long head = head();
        Event onlyEvent = anEvent(aString(), eventName);
        appendEvent(taggedEvent(onlyEvent, tag));
        addSnapshot(snapshotKey, head, snapshotPayload);

        SnapshottedSourceRequest request = SnapshottedSourceRequest.newBuilder()
                                                                   .setSnapshotKey(unknownSnapshotKey)
                                                                   .addCriterion(tagAndNameCriterion(tag, eventName))
                                                                   .build();

        StepVerifier.create(new ResultStreamPublisher<>(() -> snapshottedDcbEventChannel.source(request)))
                    .expectNextMatches(r -> r.getResultCase() == SnapshottedSourceEventsResponse.ResultCase.EVENT
                            && r.getEvent().getSequence() == head
                            && r.getEvent().getEvent().equals(onlyEvent))
                    .expectNextMatches(r -> r.getResultCase()
                            == SnapshottedSourceEventsResponse.ResultCase.CONSISTENCY_MARKER
                            && r.getConsistencyMarker() == head + 1)
                    .verifyComplete();
    }

    @Test
    void sourceOnEmptyMatchingSetReturnsOnlyConsistencyMarker() {
        Tag tag = aTag();
        String eventName = aString();
        ByteString snapshotKey = ByteString.copyFromUtf8("empty-" + UUID.randomUUID());

        long head = head();

        SnapshottedSourceRequest request = SnapshottedSourceRequest.newBuilder()
                                                                   .setSnapshotKey(snapshotKey)
                                                                   .addCriterion(tagAndNameCriterion(tag, eventName))
                                                                   .build();

        StepVerifier.create(new ResultStreamPublisher<>(() -> snapshottedDcbEventChannel.source(request)))
                    .expectNextMatches(r -> r.getResultCase()
                            == SnapshottedSourceEventsResponse.ResultCase.CONSISTENCY_MARKER
                            && r.getConsistencyMarker() == head)
                    .verifyComplete();
    }

    private long head() {
        return dcbEventChannel.head().join().getSequence();
    }

    private void appendEvent(TaggedEvent taggedEvent) {
        dcbEventChannel.append(taggedEvent).join();
    }

    private void addSnapshot(ByteString key, long sequence, ByteString payload) {
        snapshotChannel.addSnapshot(AddSnapshotRequest.newBuilder()
                                                      .setKey(key)
                                                      .setSequence(sequence)
                                                      .setPrune(false)
                                                      .setSnapshot(Snapshot.newBuilder()
                                                                           .setName("snapshot-" + sequence)
                                                                           .setVersion("1.0")
                                                                           .setPayload(payload)
                                                                           .build())
                                                      .build())
                       .join();
    }

    private static Criterion tagAndNameCriterion(Tag tag, String name) {
        return Criterion.newBuilder()
                        .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                              .addTag(tag)
                                                              .addName(name))
                        .build();
    }

    private static Tag aTag() {
        return Tag.newBuilder()
                  .setKey(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                  .setValue(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                  .build();
    }

    private static String aString() {
        return UUID.randomUUID().toString();
    }

    private static TaggedEvent taggedEvent(Event event, Tag... tags) {
        TaggedEvent.Builder builder = TaggedEvent.newBuilder().setEvent(event);
        for (Tag tag : tags) {
            builder.addTag(tag);
        }
        return builder.build();
    }

    private static Event anEvent(String eventId, String eventName) {
        return Event.newBuilder()
                    .setIdentifier(eventId)
                    .setName(eventName)
                    .setPayload(ByteString.empty())
                    .setTimestamp(Instant.now().toEpochMilli())
                    .setVersion("0.0.1")
                    .build();
    }
}
