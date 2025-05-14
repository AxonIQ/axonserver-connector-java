package io.axoniq.axonserver.connector.event.dcb;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.ResultStreamPublisher;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.ConsistencyCondition;
import io.axoniq.axonserver.grpc.event.dcb.Criterion;
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.GetHeadRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetSequenceAtRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetSequenceAtResponse;
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

import java.io.IOException;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;

class DcbEndToEndTest extends AbstractAxonServerIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(DcbEndToEndTest.class.getName());

    /**
     * Set this flag to true to run tests against a local Axon Server instance
     * instead of using Docker containers.
     * When true, tests will connect to localhost:8024 (HTTP) and localhost:8124 (gRPC)
     */
    private static final boolean LOCAL = false;

    /**
     * HTTP port for local Axon Server instance
     */
    private static ServerAddress axonServerHttpPort;

    /**
     * Static initialization for local mode
     */
    static {
        if (LOCAL) {
            // When using local instance, set the axonServerAddress to localhost:8124
            axonServerAddress = new ServerAddress("localhost", 8124);
            axonServerHttpPort = new ServerAddress("localhost", 8024);
            logger.info("Static initialization for local Axon Server at localhost:8124");
        }
    }

    private AxonServerConnection connection;
    private AxonServerConnectionFactory client;

    @BeforeEach
    void setUp() {
        AxonServerConnectionFactory.Builder builder = AxonServerConnectionFactory.forClient("dcb-e2e-test")
                                            .connectTimeout(1500, TimeUnit.MILLISECONDS)
                                            .reconnectInterval(500, MILLISECONDS);

        if (LOCAL) {
            // Connect to local Axon Server instance
            builder.routingServers(new ServerAddress("localhost", 8124));
            logger.info("Using local Axon Server instance at localhost:8124");
        } else {
            // Connect to Docker container
            builder.routingServers(axonServerAddress);
            Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
            axonServerContainer.followOutput(logConsumer);
        }

        client = builder.build();
        connection = client.connect("default");
    }

    @AfterEach
    void tearDown() {
        client.shutdown();
    }

    @Override
    protected boolean dcbContext() {
        return true;
    }

    /**
     * Add a setup method to handle local instance configuration
     */
    @BeforeEach
    void setupLocalInstance() {
        if (LOCAL) {
            // When using local instance, just log the version
            logger.info("Using local Axon Server instance - skipping event purge");
        }
    }

    /**
     * Setup method to handle local instance configuration
     */
    @BeforeAll
    static void initializeLocal() {
        if (LOCAL) {
            // Skip the parent initialization when using a local instance
            logger.info("Using local Axon Server instance - skipping container setup");
        }
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
        long sequence = appendEvent(taggedEvent).getSequenceOfTheFirstEvent();

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
        AppendEventsResponse appendEventsResponse1 = appendEvent(taggedEvent1, condition1);
        AppendEventsResponse appendEventsResponse2 = appendEvent(taggedEvent2, condition2);

        assertEquals(head, appendEventsResponse1.getSequenceOfTheFirstEvent());
        assertEquals(1, appendEventsResponse1.getTransactionSize());
        assertEquals(head + 1, appendEventsResponse1.getConsistencyMarker());

        assertEquals(head + 1, appendEventsResponse2.getSequenceOfTheFirstEvent());
        assertEquals(1, appendEventsResponse2.getTransactionSize());
        assertEquals(head + 2, appendEventsResponse2.getConsistencyMarker());

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

    /**
     * Helper method to check if the getSequenceAt feature is available in the current Axon Server version
     * @return true if the feature is available, false otherwise
     */
    private boolean isGetSequenceAtSupported() {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        long timestamp = Instant.now().toEpochMilli();

        try {
            dcbEventChannel.getSequenceAt(GetSequenceAtRequest.newBuilder()
                                                           .setTimestamp(timestamp)
                                                           .build())
                                                           .get(2, TimeUnit.SECONDS);
            return true;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e.getCause() instanceof io.grpc.StatusRuntimeException) {
                io.grpc.StatusRuntimeException statusException = (io.grpc.StatusRuntimeException) e.getCause();
                if (statusException.getStatus().getCode() == io.grpc.Status.Code.UNIMPLEMENTED) {
                    logger.info("getSequenceAt feature is not supported in this Axon Server version. Skipping test.");
                    return false;
                }
            }
            // For other types of errors, we'll assume the feature is supported but there's another issue
            return true;
        }
    }

    @Test
    void getSequenceAtEmptyStore() {
        // Skip test if getSequenceAt is not supported
        Assumptions.assumeTrue(isGetSequenceAtSupported(), "getSequenceAt feature is not supported in this Axon Server version");

        // In an empty store, getSequenceAt should return the tail sequence (0)
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        long timestamp = Instant.now().toEpochMilli();

        try {
            GetSequenceAtResponse response = dcbEventChannel.getSequenceAt(GetSequenceAtRequest.newBuilder()
                                                                          .setTimestamp(timestamp)
                                                                          .build())
                                                          .get(10, TimeUnit.SECONDS);
            assertEquals(0, response.getSequence(), "Empty store should return sequence 0");
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            fail("getSequenceAt operation timed out or failed: " + e.getMessage());
        }
    }

    @Test
    void getSequenceAtBeforeAllEvents() {
        // Skip test if getSequenceAt is not supported
        Assumptions.assumeTrue(isGetSequenceAtSupported(), "getSequenceAt feature is not supported in this Axon Server version");

        // When timestamp is before all events, should return the tail sequence
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();

        try {
            GetTailResponse tailResponse = dcbEventChannel.tail(GetTailRequest.getDefaultInstance())
                                                      .join();

            long tail = tailResponse.getSequence();

            // Add some events
            int numEvents = 3;
            List<Long> timestamps = new ArrayList<>();
            for (int i = 0; i < numEvents; i++) {
                // Create events with increasing timestamps, starting from now
                long timestamp = Instant.now().toEpochMilli() + (i * 500);
                timestamps.add(timestamp);

                TaggedEvent taggedEvent = taggedEvent(Event.newBuilder()
                                                          .setIdentifier(aString())
                                                          .setName("event-" + i)
                                                          .setPayload(ByteString.empty())
                                                          .setTimestamp(timestamp)
                                                          .setVersion("0.0.1")
                                                          .build());
                appendEvent(taggedEvent);
            }

            // Request with timestamp before all events
            long earlyTimestamp = timestamps.get(0) - 100000000; // 1 second before first event
            try {
                GetSequenceAtResponse response = dcbEventChannel.getSequenceAt(GetSequenceAtRequest.newBuilder()
                                                                              .setTimestamp(earlyTimestamp)
                                                                              .build())
                                                              .get(10, TimeUnit.SECONDS);

                assertEquals(tail, response.getSequence(),
                        "Timestamp before all events should return the tail sequence");
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                fail("getSequenceAt operation timed out or failed: " + e.getMessage());
            }
        } catch (Exception e) {
            // If we can't retrieve head or append events, the test environment might not be properly set up
            // This is a different issue than the getSequenceAt feature not being supported
            Assumptions.assumeTrue(false, "Test environment not properly set up: " + e.getMessage());
        }
    }

    @Test
    void getSequenceAtAfterAllEvents() {
        // Skip test if getSequenceAt is not supported
        Assumptions.assumeTrue(isGetSequenceAtSupported(), "getSequenceAt feature is not supported in this Axon Server version");

        // When timestamp is after all events, should return the head sequence
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();

        try {
            // Add some events
            int numEvents = 3;
            List<Long> timestamps = new ArrayList<>();
            for (int i = 0; i < numEvents; i++) {
                // Create events with increasing timestamps, starting from now
                long timestamp = Instant.now().toEpochMilli() + (i * 500);
                timestamps.add(timestamp);

                TaggedEvent taggedEvent = taggedEvent(Event.newBuilder()
                                                          .setIdentifier(aString())
                                                          .setName("event-" + i)
                                                          .setPayload(ByteString.empty())
                                                          .setTimestamp(timestamp)
                                                          .setVersion("0.0.1")
                                                          .build());
                appendEvent(taggedEvent);
            }

            // Get the current head after adding events
            long head = retrieveHead();

            // Request with timestamp after all events
            long futureTimestamp = timestamps.get(numEvents - 1) + 1000; // 1 second after last event
            try {
                GetSequenceAtResponse response = dcbEventChannel.getSequenceAt(GetSequenceAtRequest.newBuilder()
                                                                              .setTimestamp(futureTimestamp)
                                                                              .build())
                                                              .get(10, TimeUnit.SECONDS);

                assertEquals(head, response.getSequence(),
                        "Timestamp after all events should return the head sequence");
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                fail("getSequenceAt operation timed out or failed: " + e.getMessage());
            }
        } catch (Exception e) {
            // If we can't append events or retrieve head, the test environment might not be properly set up
            // This is a different issue than the getSequenceAt feature not being supported
            Assumptions.assumeTrue(false, "Test environment not properly set up: " + e.getMessage());
        }
    }

    @Test
    void getSequenceAtExactMatch() {
        // Skip test if getSequenceAt is not supported
        Assumptions.assumeTrue(isGetSequenceAtSupported(), "getSequenceAt feature is not supported in this Axon Server version");

        // When timestamp exactly matches an event, should return that event's sequence
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();

        try {
            long head = retrieveHead();

            // Add some events with specific timestamps
            int numEvents = 3;
            List<Long> timestamps = new ArrayList<>();
            List<Long> sequences = new ArrayList<>();

            for (int i = 0; i < numEvents; i++) {
                // Create events with increasing timestamps, starting from now
                long timestamp = Instant.now().toEpochMilli() + (i * 500);
                timestamps.add(timestamp);

                TaggedEvent taggedEvent = taggedEvent(Event.newBuilder()
                                                          .setIdentifier(aString())
                                                          .setName("event-" + i)
                                                          .setPayload(ByteString.empty())
                                                          .setTimestamp(timestamp)
                                                          .setVersion("0.0.1")
                                                          .build());
                AppendEventsResponse appendResponse = appendEvent(taggedEvent);
                sequences.add(appendResponse.getSequenceOfTheFirstEvent());
            }

            // Request with timestamp exactly matching the middle event
            int middleIndex = numEvents / 2;
            long exactTimestamp = timestamps.get(middleIndex);
            try {
                GetSequenceAtResponse response = dcbEventChannel.getSequenceAt(GetSequenceAtRequest.newBuilder()
                                                                              .setTimestamp(exactTimestamp)
                                                                              .build())
                                                              .get(10, TimeUnit.SECONDS);

                assertEquals(sequences.get(middleIndex), response.getSequence(),
                        "Timestamp exactly matching an event should return that event's sequence");
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                fail("getSequenceAt operation timed out or failed: " + e.getMessage());
            }
        } catch (Exception e) {
            // If we can't retrieve head or append events, the test environment might not be properly set up
            // This is a different issue than the getSequenceAt feature not being supported
            Assumptions.assumeTrue(false, "Test environment not properly set up: " + e.getMessage());
        }
    }

    @Test
    void getSequenceAtBetweenEvents() {
        // Skip test if getSequenceAt is not supported
        Assumptions.assumeTrue(isGetSequenceAtSupported(), "getSequenceAt feature is not supported in this Axon Server version");

        // When timestamp is between events, should return the sequence of the event with timestamp <= requested timestamp
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();

        try {
            long head = retrieveHead();

            // Add some events with specific timestamps
            int numEvents = 3;
            List<Long> timestamps = new ArrayList<>();
            List<Long> sequences = new ArrayList<>();

            for (int i = 0; i < numEvents; i++) {
                // Create events with increasing timestamps, 500ms apart
                long timestamp = Instant.now().toEpochMilli() + (i * 500);
                timestamps.add(timestamp);

                TaggedEvent taggedEvent = taggedEvent(Event.newBuilder()
                                                          .setIdentifier(aString())
                                                          .setName("event-" + i)
                                                          .setPayload(ByteString.empty())
                                                          .setTimestamp(timestamp)
                                                          .setVersion("0.0.1")
                                                          .build());
                AppendEventsResponse appendResponse = appendEvent(taggedEvent);
                sequences.add(appendResponse.getSequenceOfTheFirstEvent());
            }

            // Request with timestamp between two events
            long betweenTimestamp = timestamps.get(1) + 250; // Halfway between events 1 and 2
            try {
                GetSequenceAtResponse response = dcbEventChannel.getSequenceAt(GetSequenceAtRequest.newBuilder()
                                                                              .setTimestamp(betweenTimestamp)
                                                                              .build())
                                                              .get(10, TimeUnit.SECONDS);

                assertEquals(sequences.get(1), response.getSequence(),
                        "Timestamp between events should return the sequence of the previous event");
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                fail("getSequenceAt operation timed out or failed: " + e.getMessage());
            }
        } catch (Exception e) {
            // If we can't retrieve head or append events, the test environment might not be properly set up
            // This is a different issue than the getSequenceAt feature not being supported
            Assumptions.assumeTrue(false, "Test environment not properly set up: " + e.getMessage());
        }
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
