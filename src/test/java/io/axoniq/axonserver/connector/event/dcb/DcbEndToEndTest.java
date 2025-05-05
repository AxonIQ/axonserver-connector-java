package io.axoniq.axonserver.connector.event.dcb;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.ResultStreamPublisher;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.ConsistencyCondition;
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
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsResponse;
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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
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

    @Override
    protected boolean dcbContext() {
        return true;
    }

    @Test
    void streamWhileAppendingWithASingleTagCriterion() {
        long head = retrieveHead();
        Tag tag = aTag();
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
    void streamWhileAppendingWithTwoCriterionsWithTwoTags() {
        long head = retrieveHead();
        Tag tag1 = aTag();
        Tag tag2 = aTag();
        Tag tag3 = aTag();
        Tag tag4 = aTag();
        Criterion criterion1 = Criterion.newBuilder()
                                        .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                                              .addTag(tag1)
                                                                              .addTag(tag2))
                                        .build();
        Criterion criterion2 = Criterion.newBuilder()
                                        .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                                              .addTag(tag3)
                                                                              .addTag(tag4))
                                        .build();
        StreamEventsRequest streamRequest = StreamEventsRequest.newBuilder()
                                                               .setFromSequence(head)
                                                               .addCriterion(criterion1)
                                                               .addCriterion(criterion2)
                                                               .build();

        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();

        int num = 1_000;
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        try {
            for (int i = 0; i < num; i++) {
                String id = UUID.randomUUID().toString();
                Event event = anEvent(id, "event-to-be-streamed");
                TaggedEvent taggedEvent = i % 2 == 0 ? taggedEvent(event, tag1, tag2) : taggedEvent(event, tag3, tag4);
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
    void streamWhileAppendingWithNoCriteria() {
        long head = retrieveHead();
        Tag tag = aTag();
        StreamEventsRequest streamRequest = StreamEventsRequest.newBuilder()
                                                               .setFromSequence(head)
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
    void streamBeforeAppends() throws InterruptedException {
        long head = retrieveHead();
        Tag tag = aTag();
        StreamEventsRequest streamRequest = StreamEventsRequest.newBuilder()
                                                               .setFromSequence(head)
                                                               .build();

        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();

        int num = 1_000;
        CountDownLatch latch = new CountDownLatch(1);
        List<Long> receivedSequences = new CopyOnWriteArrayList<>();

        fluxStream(() -> dcbEventChannel.stream(streamRequest))
                .take(num)
                .map(r -> r.getEvent().getSequence())
                .doOnNext(receivedSequences::add)
                .doOnComplete(latch::countDown)
                .subscribe();
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        try {
            for (int i = 0; i < num; i++) {
                String id = UUID.randomUUID().toString();
                TaggedEvent taggedEvent = taggedEvent(anEvent(id, "event-to-be-streamed"), tag);
                executorService.submit(() -> appendEvent(taggedEvent));
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS));
            List<Long> expectedSequences = LongStream.range(head, head + num)
                                                     .boxed()
                                                     .collect(Collectors.toList());
            assertEquals(expectedSequences, receivedSequences);
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    void streamAfterAppends() throws InterruptedException {
        long head = retrieveHead();
        Tag tag = aTag();
        StreamEventsRequest streamRequest = StreamEventsRequest.newBuilder()
                                                               .setFromSequence(head)
                                                               .build();

        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();

        int num = 1_000;
        CountDownLatch latch = new CountDownLatch(num);
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        try {
            for (int i = 0; i < num; i++) {
                String id = UUID.randomUUID().toString();
                TaggedEvent taggedEvent = taggedEvent(anEvent(id, "event-to-be-streamed"), tag);
                executorService.submit(() -> {
                    appendEvent(taggedEvent);
                    latch.countDown();
                });
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS));

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
                    .expectNextMatches(response -> head == response.getEvent().getSequence()
                            && response.getEvent().getEvent().equals(events.get(0)))
                    .expectNextMatches(response -> head + 2 == response.getEvent().getSequence()
                            && response.getEvent().getEvent().equals(events.get(2)))
                    .expectNextMatches(response -> head + 4 == response.getEvent().getSequence()
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
        long head = retrieveHead();
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        String eventName = aString();

        TaggedEvent taggedEvent = taggedEvent(anEvent(aString(), eventName));
        appendEvent(taggedEvent);

        Criterion typeCriterion = criterionWithOnlyName(eventName);

        SourceEventsRequest request = SourceEventsRequest.newBuilder()
                                                         .addCriterion(typeCriterion)
                                                         .build();

        StepVerifier.create(new ResultStreamPublisher<>(() -> dcbEventChannel.source(request)))
                    .expectNextMatches(r -> r.getEvent().getEvent().equals(taggedEvent.getEvent()))
                    .expectNextMatches(r -> r.getConsistencyMarker() == head + 1L)
                    .verifyComplete();
    }

    @Test
    void tagsFor() {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();

        Tag tag = aTag();
        TaggedEvent taggedEvent = taggedEvent(anEvent(aString(), "myName"), tag);
        long sequence = appendEvent(taggedEvent).getLastPosition();

        GetTagsResponse response = dcbEventChannel.tagsFor(GetTagsRequest.newBuilder()
                                                                         .setSequence(sequence)
                                                                         .build())
                                                  .join();
        assertEquals(ImmutableList.of(tag), response.getTagList());
    }

    @Test
    void head() {
        assertEquals(0, retrieveHead());
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
        long head = retrieveHead();
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
        StepVerifier.create(new ResultStreamPublisher<>(() -> dcbEventChannel.source(sourceEventsRequest(head))))
                    .expectNextMatches(e -> e.getEvent().getEvent().equals(taggedEvent1.getEvent()))
                    .expectNextMatches(e -> e.getEvent().getEvent().equals(taggedEvent2.getEvent()))
                    .expectNextMatches(e -> e.getConsistencyMarker() == head + 2L)
                    .verifyComplete();
    }

    @Test
    void twoClashingAppendsWithTheSameCondition() {
        String eventName = aString();
        long head = retrieveHead();
        Tag tag = aTag();

        TaggedEvent taggedEvent = taggedEvent(anEvent(aString(), eventName), tag);
        ConsistencyCondition condition =
                ConsistencyCondition.newBuilder()
                                    .setConsistencyMarker(head)
                                    .addCriterion(Criterion.newBuilder()
                                                           .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                                                                 .addTag(tag))
                                                           .build())
                                    .build();
        appendEvent(taggedEvent, condition);
        appendEventAsync(taggedEvent, condition)
                .handle((res, err) -> {
                    assertEquals("CANCELLED: io.axoniq.axonserver.eventstore.api.ConsistencyConditionException: Consistency condition is not met.", err.getMessage());
                    return null;
                })
                .join();
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
    void streamAtTheHead() {
        long head = retrieveHead();

        StepVerifier.create(streamFlux(head).take(Duration.ofSeconds(5)))
                    .verifyComplete();
    }

    private Flux<SourceEventsResponse> sourceFlux(long start) {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        return fluxStream(() -> dcbEventChannel.source(sourceEventsRequest(start)));
    }

    private Flux<StreamEventsResponse> streamFlux(long start) {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        return fluxStream(() -> dcbEventChannel.stream(streamEventsRequest(start)));
    }

    private <T> Flux<T> fluxStream(Supplier<ResultStream<T>> stream) {
        return Flux.from(new ResultStreamPublisher<>(stream));
    }

    private static Tag aTag() {
        return tag(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    private static String aString() {
        return UUID.randomUUID().toString();
    }

    private static StreamEventsRequest streamEventsRequest(long from) {
        return StreamEventsRequest.newBuilder()
                                  .setFromSequence(from)
                                  .build();
    }

    private static SourceEventsRequest sourceEventsRequest(long from) {
        return SourceEventsRequest.newBuilder()
                                  .setFromSequence(from)
                                  .build();
    }

    private static Tag tag(String key, String value) {
        return Tag.newBuilder()
                  .setKey(ByteString.copyFromUtf8(key))
                  .setValue(ByteString.copyFromUtf8(value))
                  .build();
    }

    private static TaggedEvent taggedEvent(Event event, Tag... tag) {
        return TaggedEvent.newBuilder()
                          .setEvent(event)
                          .addAllTag(Arrays.asList(tag))
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

    private AppendEventsResponse appendEvent(TaggedEvent taggedEvent) {
        return appendEventAsync(taggedEvent).join();
    }

    private AppendEventsResponse appendEvent(TaggedEvent taggedEvent, ConsistencyCondition condition) {
        return appendEventAsync(taggedEvent, condition).join();
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
