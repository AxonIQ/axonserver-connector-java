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

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
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
        connection.disconnect();
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
                                                               .addCriterion(
                                                                       criterion)
                                                               .build();

        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();

        AtomicLong received = new AtomicLong();
        int num = 1_000;
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        try {
            for (int i = 0; i < num; i++) {
                String id = UUID.randomUUID().toString();
                TaggedEvent taggedEvent = taggedEvent(anEvent(id, "event-to-be-streamed"), tag);
                executorService.submit(() -> appendEvent(taggedEvent));
            }

            Flux.from(new ResultStreamPublisher<>(() -> dcbEventChannel.stream(streamRequest)))
                .take(num)
                .doOnNext(e -> {
                    long expected = head + received.getAndIncrement();
                    long current = e.getEvent().getSequence();
                    if (expected != current) {
                        throw new AssertionError(format("Expected %d received %d", expected, current));
                    }
                })
                .blockLast(Duration.ofMinutes(1));

            assertEquals(num, received.get());
        } finally {
            executorService.shutdown();
        }
    }


    @Test
    void noConditionAppend() throws InterruptedException {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        String eventName = "myUniqueNameNobodyElseWillUse" + UUID.randomUUID();

        TaggedEvent taggedEvent = taggedEvent(anEvent(UUID.randomUUID().toString(), eventName));
        appendEvent(taggedEvent);

        Criterion typeCriterion = criterionWithOnlyName(eventName);
        SourceEventsResponse sourceResponse = dcbEventChannel.source(SourceEventsRequest.newBuilder()
                                                                                        .addCriterion(typeCriterion)
                                                                                        .build())
                                                             .nextIfAvailable(1, SECONDS);
        Event receivedEvent = sourceResponse.getEvent().getEvent();
        assertEquals(taggedEvent.getEvent(), receivedEvent);
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

    private long retrieveHead() {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        return dcbEventChannel.head(GetHeadRequest.getDefaultInstance())
                              .join()
                              .getSequence();
    }

    private void appendEvent(TaggedEvent taggedEvent) {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        dcbEventChannel.startTransaction()
                       .append(taggedEvent)
                       .commit()
                       .join();
    }

    @Test
    void twoNonClashingAppends() {
//        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
//        String eventId = "what";
//        String myType = "myTypeThatIsNotTheSameAsInTheCriterion";
//
//        TypedEventPayload typedEventPayload = typedEventPayload(eventId, myType);
//
//        TaggedEventPayload taggedEvent = TaggedEventPayload.newBuilder()
//                                                           .setTypedEvent(typedEventPayload)
//                                                           .build();
//
//
//        AppendRequest.Event event = AppendRequest.Event.newBuilder()
//                                                       .setEvent(taggedEvent)
//                                                       .build();
//        String key1 = "key1";
//        String value1 = "value1";
//        Criterion criterion1 = criterionWithOneTag(key1, value1);
//        Position consistencyMarker = Position.newBuilder()
//                                             .setSequence(0L)
//                                             .build();
//        ConsistencyCondition condition1 = ConsistencyCondition.newBuilder()
//                                                              .setConsistencyMarker(consistencyMarker)
//                                                              .addCriterion(criterion1)
//                                                              .build();
//        AppendResponse response1 = dcbEventChannel.startTransaction()
//                                                  .append(event)
//                                                  .condition(condition1)
//                                                  .commit()
//                                                  .join();
//
//        assertEquals(0L, response1.getLastPosition().getSequence());
//
//        Tag tag2 = Tag.newBuilder()
//                      .setKey(ByteString.copyFromUtf8("key2"))
//                      .setValue(ByteString.copyFromUtf8("value2"))
//                      .build();
//        Criterion criterion2 = Criterion.newBuilder()
//                                        .setTagsAndTypes(TagsAndTypesCriterion.newBuilder()
//                                                                              .addType("type")
//                                                                              .addTag(tag2)
//                                                                              .build())
//                                        .build();
//        ConsistencyCondition condition2 = ConsistencyCondition.newBuilder()
//                                                              .setConsistencyMarker(consistencyMarker)
//                                                              .addCriterion(criterion2)
//                                                              .build();
//        AppendResponse response2 = dcbEventChannel.startTransaction()
//                                                  .append(event)
//                                                  .condition(condition2)
//                                                  .commit()
//                                                  .join();
//
//        assertEquals(1L, response2.getLastPosition().getSequence());
    }

//    private static @NotNull Criterion criterionWithOneTag(String key1, String value1) {
//        Tag tag1 = Tag.newBuilder()
//                      .setKey(ByteString.copyFromUtf8(key1))
//                      .setValue(ByteString.copyFromUtf8(value1))
//                      .build();
//        Criterion criterion1 = Criterion.newBuilder()
//                                        .setTagsAndTypes(TagsAndTypesCriterion.newBuilder()
//                                                                              .addType("type")
//                                                                              .addTag(tag1)
//                                                                              .build())
//                                        .build();
//        return criterion1;
//    }

    @Test
    void twoClashingAppends() {

    }

    @Test
    void concurrentAppends() {

    }

    @Test
    void transactionRollback() {

    }

    @Test
    void sourceEmptyEventStore() {

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
}
