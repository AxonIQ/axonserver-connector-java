package io.axoniq.axonserver.connector.event.dcb;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.ResultStreamPublisher;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.grpc.event.dcb.Criterion;
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsResponse;
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
import java.util.concurrent.TimeUnit;

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
    }

    @AfterEach
    void tearDown() {
        connection.disconnect();
        client.shutdown();
    }

    @Test
    void append() {
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
        axonServerContainer.followOutput(logConsumer);
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        ResultStream<StreamEventsResponse> stream = dcbEventChannel.stream(StreamEventsRequest.newBuilder()
                                                                                              .setFromSequence(0L)
                                                                                              .build());
        Flux.from(new ResultStreamPublisher<>(() -> stream))
            .subscribe(e -> {
                if (e.getEvent().getSequence() % 10_000 == 0) {
                    System.out.println(e.getEvent().getSequence());
                }
            });
        Instant start = Instant.now();
        int num = 100_000;
        for (int i = 0; i < num; i++) {
            String id = UUID.randomUUID().toString();
            TaggedEvent taggedEvent = TaggedEvent.newBuilder()
                                                 .setEvent(Event.newBuilder()
                                                                .setIdentifier(id)
                                                                .setName("my-event")
                                                                .setPayload(ByteString.empty())
                                                                .setTimestamp(Instant.now().toEpochMilli())
                                                                .setVersion("0.0.1")
                                                                .build())
                                                 .build();
            dcbEventChannel.startTransaction()
                           .append(taggedEvent)
                           .commit()
                           .join();
        }
        logger.info("Appended {} events in: {}", num, Duration.between(start, Instant.now()));

    }

    @Test
    void noConditionAppend() throws InterruptedException {
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
        axonServerContainer.followOutput(logConsumer);

        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        String eventId = "what";
        String myName = "myUniqueNameNobodyElseWillUse" + UUID.randomUUID();

        TaggedEvent taggedEvent = TaggedEvent.newBuilder()
                                             .setEvent(Event.newBuilder()
                                                            .setIdentifier(eventId)
                                                            .setName(myName)
                                                            .setPayload(ByteString.empty())
                                                            .setTimestamp(Instant.now().toEpochMilli())
                                                            .setVersion("0.0.1")
                                                            .build())
                                             .build();
        dcbEventChannel.startTransaction()
                       .append(taggedEvent)
                       .commit()
                       .join();

        Criterion typeCriterion = criterionWithOnlyName(myName);
        SourceEventsResponse sourceResponse = dcbEventChannel.source(SourceEventsRequest.newBuilder()
                                                                                        .addCriterion(typeCriterion)
                                                                                        .build())
                                                             .nextIfAvailable(1, SECONDS);
        Event receivedEvent = sourceResponse.getEvent().getEvent();
        assertEquals(myName, receivedEvent.getName());
        assertEquals(eventId, receivedEvent.getIdentifier());
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


    private static Criterion criterionWithOnlyName(String myname) {
        TagsAndNamesCriterion ttc = TagsAndNamesCriterion.newBuilder()
                                                         .addName(myname)
                                                         .build();
        return Criterion.newBuilder().setTagsAndNames(ttc).build();
    }
}
