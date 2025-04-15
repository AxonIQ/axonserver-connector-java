package io.axoniq.axonserver.connector.event.dcb;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStreamPublisher;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.grpc.event.dcb.Criterion;
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.GetHeadRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetTagsRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetTagsResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetTailRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetTailResponse;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.Tag;
import io.axoniq.axonserver.grpc.event.dcb.TaggedEvent;
import io.axoniq.axonserver.grpc.event.dcb.TagsAndNamesCriterion;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;

class DcbEndToEndTest extends AbstractAxonServerIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(DcbEndToEndTest.class.getName());

    private AxonServerConnection connection;
    private AxonServerConnectionFactory client;

    @BeforeEach
    void setUp() {
        client = AxonServerConnectionFactory.forClient("dcb-e2e-test")
                                            .connectTimeout(1500, TimeUnit.MILLISECONDS)
                                            .reconnectInterval(500, MILLISECONDS)
                                            .routingServers(axonServerAddress)
                                            .build();
        connection = client.connect("default");
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
        axonServerContainer.followOutput(logConsumer);
    }

    @AfterEach
    void tearDown() {
        client.shutdown();
    }

    @Test
    void stream() {
        long head = retrieveHead();
        Tag tag = tag(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        Criterion criterion = Criterion.newBuilder()
                                       .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                                             .addTag(tag))
                                       .build();
        StreamEventsRequest streamRequest = StreamEventsRequest.newBuilder()
                                                               .setFromSequence(head)
                                                               .addCriterion(criterion)
                                                               .build();

        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();

        int num = 1_000;
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        try {
            for (int i = 0; i < num; i++) {
                String id = UUID.randomUUID().toString();
                TaggedEvent taggedEvent = taggedEvent(anEvent(id, "event-to-be-streamed"), tag);
                executorService.submit(() -> appendEvent(taggedEvent));
            }

            StepVerifier.create(fluxStream(() -> dcbEventChannel.stream(streamRequest))
                                        .take(num)
                                        .map(r -> r.getEvent().getSequence()))
                        .expectNextSequence(LongStream.range(head, head + num)
                                                      .boxed()
                                                      .collect(Collectors.toList()))
                        .verifyComplete();
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    void source() {
        long head = retrieveHead();
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        Tag tag = aTag();
        String eventName = aString();
        int num = 6;
        List<Event> events = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            TaggedEvent taggedEvent = i % 2 == 0
                    ? taggedEvent(anEvent(aString(), eventName), tag)
                    : taggedEvent(anEvent(aString(), eventName));
            events.add(taggedEvent.getEvent());
            appendEvent(taggedEvent);
        }

        Criterion criterion = Criterion.newBuilder()
                                       .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                                             .addTag(tag)
                                                                             .addName(eventName))
                                       .build();
        SourceEventsRequest request = SourceEventsRequest.newBuilder()
                                                         .addCriterion(criterion)
                                                         .build();
        StepVerifier.create(new ResultStreamPublisher<>(() -> dcbEventChannel.source(request)))
                    .expectNextMatches(response -> 0 == response.getEvent().getSequence()
                            && response.getEvent().getEvent().equals(events.get(0)))
                    .expectNextMatches(response -> 2 == response.getEvent().getSequence()
                            && response.getEvent().getEvent().equals(events.get(2)))
                    .expectNextMatches(response -> 4 == response.getEvent().getSequence()
                            && response.getEvent().getEvent().equals(events.get(4)))
                    .expectNextMatches(response -> head + num == response.getConsistencyMarker())
                    .verifyComplete();
    }

    @Test
    void sourceAnEmptyEventStore() {
        long head = retrieveHead();
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        Tag tag = aTag();
        String eventName = aString();

        Criterion criterion = Criterion.newBuilder()
                                       .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                                             .addTag(tag)
                                                                             .addName(eventName))
                                       .build();
        SourceEventsRequest request = SourceEventsRequest.newBuilder()
                                                         .addCriterion(criterion)
                                                         .build();
        StepVerifier.create(new ResultStreamPublisher<>(() -> dcbEventChannel.source(request)))
                    .expectNextMatches(response -> head == response.getConsistencyMarker())
                    .verifyComplete();
    }

    @Test
    void sourceSingleTagAndEventName() {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        String eventName = aString();
        Tag tag = aTag();

        long head = retrieveHead();
        TaggedEvent taggedEvent = taggedEvent(anEvent(aString(), eventName), tag);
        appendEvent(taggedEvent);

        Criterion typeAndTagCriterion = Criterion.newBuilder()
                                                 .setTagsAndNames(
                                                         TagsAndNamesCriterion.newBuilder()
                                                                              .addTag(tag)
                                                                              .addName(eventName)
                                                                              .build()
                                                 ).build();

        SourceEventsRequest request = SourceEventsRequest.newBuilder()
                                                         .addCriterion(typeAndTagCriterion)
                                                         .build();
        StepVerifier.create(new ResultStreamPublisher<>(() -> dcbEventChannel.source(request)))
                    .expectNextMatches(r -> r.getEvent().getEvent().equals(taggedEvent.getEvent())
                            && r.getEvent().getSequence() == head)
                    .expectNext(SourceEventsResponse.newBuilder().setConsistencyMarker(head + 1).build())
                    .verifyComplete();
    }

    @Test
    void noConditionAppend() {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        String eventName = "myUniqueNameNobodyElseWillUse" + UUID.randomUUID();

        TaggedEvent taggedEvent = taggedEvent(anEvent(UUID.randomUUID().toString(), eventName));
        appendEvent(taggedEvent);

        Criterion typeCriterion = criterionWithOnlyName(eventName);

        SourceEventsRequest request = SourceEventsRequest.newBuilder()
                                                         .addCriterion(typeCriterion)
                                                         .build();

        StepVerifier.create(new ResultStreamPublisher<>(() -> dcbEventChannel.source(request)))
                    .expectNextMatches(r -> r.getEvent().getEvent().equals(taggedEvent.getEvent()))
                    .expectNextMatches(r -> r.getConsistencyMarker() == 1L)
                    .verifyComplete();
    }

    @Test
    void tagsFor() {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();

        Tag tag = tag(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        TaggedEvent taggedEvent = taggedEvent(anEvent(UUID.randomUUID().toString(), "myName"), tag);
        appendEvent(taggedEvent);

        GetTagsResponse response = dcbEventChannel.tagsFor(GetTagsRequest.newBuilder()
                                                                         .setSequence(0L)
                                                                         .build())
                                                  .join();
        assertEquals(ImmutableList.of(tag), response.getTagList());
    }

    @Test
    void head() {
        assertEquals(0, retrieveHead());

        TaggedEvent taggedEvent = taggedEvent(anEvent(UUID.randomUUID().toString(), "myName"));
        appendEvent(taggedEvent);

        assertEquals(1, retrieveHead());
    }

    @Test
    void tail() {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        GetTailResponse response = dcbEventChannel.tail(GetTailRequest.getDefaultInstance())
                                                  .join();
        assertEquals(0, response.getSequence());
    }

    @Test
    void twoNonClashingAppends() {
        Tag tag1 = aTag();
        Tag tag2 = aTag();
        TaggedEvent taggedEvent1 = taggedEvent(anEvent(aString(), aString()), tag1);
        TaggedEvent taggedEvent2 = taggedEvent(anEvent(aString(), aString()), tag2);
        ConsistencyCondition condition1 =
                ConsistencyCondition.newBuilder()
                                    .addCriterion(Criterion.newBuilder()
                                                           .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                                                                 .addTag(tag1)))
                                    .build();
        ConsistencyCondition condition2 =
                ConsistencyCondition.newBuilder()
                                    .addCriterion(Criterion.newBuilder()
                                                           .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                                                                 .addTag(tag2)))
                                    .build();
        appendEvent(taggedEvent1, condition1);
        appendEvent(taggedEvent2, condition2);

        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        StepVerifier.create(new ResultStreamPublisher<>(() -> dcbEventChannel.source(sourceEventsRequest(0L))))
                    .expectNextMatches(e -> e.getEvent().getEvent().equals(taggedEvent1.getEvent()))
                    .expectNextMatches(e -> e.getEvent().getEvent().equals(taggedEvent2.getEvent()))
                    .expectNextMatches(e -> e.getConsistencyMarker() == 2L)
                    .verifyComplete();
    }

    @Test
    void twoClashingAppends() {

    }

    @Test
    void concurrentAppends() throws InterruptedException {
        long head = retrieveHead();
        Tag tag = aTag();
        String eventName = aString();
        int num = 1_000;
        ExecutorService executorService = Executors.newFixedThreadPool(16);

        Criterion criterion = Criterion.newBuilder()
                                       .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                                             .addName(eventName)
                                                                             .addTag(tag)
                                                                             .build())
                                       .build();

        ConcurrentSkipListSet<String> successfulIds = new ConcurrentSkipListSet<>();
        CountDownLatch latch = new CountDownLatch(num);

        try {
            for (int i = 0; i < num; i++) {
                String id = aString();
                TaggedEvent taggedEvent = taggedEvent(anEvent(id, eventName), tag);
                long consistencyMarker = head + (i % 100);
                ConsistencyCondition condition = ConsistencyCondition.newBuilder()
                                                                     .setConsistencyMarker(consistencyMarker)
                                                                     .addCriterion(criterion)
                                                                     .build();
                executorService.submit(() -> {
                    try {
                        appendEvent(taggedEvent, condition);
                        successfulIds.add(id);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
            Set<String> receivedIds = sourceFlux(head)
                    .filter(SourceEventsResponse::hasEvent)
                    .map(r -> r.getEvent().getEvent().getIdentifier())
                    .collect(Collectors.toSet())
                    .block();

            assertEquals(successfulIds, receivedIds);
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    void transactionRollback() {
        long head = retrieveHead();
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        TaggedEvent taggedEvent = taggedEvent(anEvent(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        dcbEventChannel.startTransaction()
                       .append(taggedEvent)
                       .rollback();
        StepVerifier.create(sourceFlux(head))
                    .expectNextMatches(r -> head == r.getConsistencyMarker())
                    .verifyComplete();
    }

    @Test
    void sourceAtTheHead() {
        long head = retrieveHead();

        StepVerifier.create(sourceFlux(head))
                    .expectNextMatches(r -> r.getConsistencyMarker() == head)
                    .verifyComplete();
    }

    @Test
    void streamEmptyEventStore() {

    }

    private static Tag tag(String key, String value) {
        return Tag.newBuilder()
                  .setKey(ByteString.copyFromUtf8(key))
                  .setValue(ByteString.copyFromUtf8(value))
                  .build();
    }

    private static TaggedEvent taggedEvent(Event event, Tag tag) {
        return TaggedEvent.newBuilder()
                          .setEvent(event)
                          .addTag(tag)
                          .build();
    }

    private static TaggedEvent taggedEvent(Event event) {
        return TaggedEvent.newBuilder()
                          .setEvent(event)
                          .build();
    }

    private static Criterion criterionWithOnlyName(String myname) {
        TagsAndNamesCriterion ttc = TagsAndNamesCriterion.newBuilder()
                                                         .addName(myname)
                                                         .build();
        return Criterion.newBuilder().setTagsAndNames(ttc).build();
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

    private long retrieveHead() {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        return dcbEventChannel.head(GetHeadRequest.getDefaultInstance())
                              .join()
                              .getSequence();
    }

    private void appendEvent(TaggedEvent taggedEvent) {
        appendEventAsync(taggedEvent).join();
    }

    private void appendEvent(TaggedEvent taggedEvent, ConsistencyCondition condition) {
        appendEventAsync(taggedEvent, condition).join();
    }

    private CompletableFuture<AppendEventsResponse> appendEventAsync(TaggedEvent taggedEvent) {
        return connection.dcbEventChannel()
                         .startTransaction()
                         .append(taggedEvent)
                         .commit();
    }

    private CompletableFuture<AppendEventsResponse> appendEventAsync(TaggedEvent taggedEvent,
                                                                     ConsistencyCondition condition) {
        return connection.dcbEventChannel()
                         .startTransaction()
                         .append(taggedEvent)
                         .condition(condition)
                         .commit();
    }
}
