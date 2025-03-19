package io.axoniq.axonserver.connector.event.dcb;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.ConsistencyCondition;
import io.axoniq.axonserver.grpc.event.dcb.Criterion;
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.Tag;
import io.axoniq.axonserver.grpc.event.dcb.TaggedEvent;
import io.axoniq.axonserver.grpc.event.dcb.TagsAndNamesCriterion;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

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
        // milan's test
    void noConditionAppend() throws InterruptedException {
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
        axonServerContainer.followOutput(logConsumer);


        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        String eventId = "what";
        String myType = "myUniqueTypeNobodyElseWillUse" + UUID.randomUUID();

        Event event = Event.newBuilder()
                           .setIdentifier(eventId)
                           .setName(myType)
                           .setPayload(ByteString.empty())
                           .putMetadata("Hello", "World")
                           .build();
        TaggedEvent taggedEvent = TaggedEvent.newBuilder()
                                             .setEvent(event)
                                             .build();
        AppendEventsResponse _unused = dcbEventChannel.startTransaction()
                                                      .append(taggedEvent)
                                                      .commit()
                                                      .join();

        Criterion typeCriterion = criterionWithOnlyType(myType);
        ResultStream<SourceEventsResponse> responseStream = dcbEventChannel.source(SourceEventsRequest.newBuilder()
                                                                                                      .addCriterion(
                                                                                                              typeCriterion)
                                                                                                      .build());
        SourceEventsResponse secondResponse = responseStream.nextIfAvailable(20, SECONDS);
        assertEquals(myType, secondResponse.getEvent().getEvent().getName()); // the event that matches criteria
        assertEquals(eventId, secondResponse.getEvent().getEvent().getIdentifier());
        SourceEventsResponse firstResponse = responseStream.nextIfAvailable(20, SECONDS);
        assertEquals(1, firstResponse.getConsistencyMarker()); // Consistency marker from onComplete
    }

    @Test
    void noConditionAppendWithTag() throws InterruptedException {
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
        axonServerContainer.followOutput(logConsumer);


        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        String eventId = "what";
        String myType = "myUniqueTypeNobodyElseWillUse" + UUID.randomUUID();
        String myUsername = "YourTest";
        String key = "myUsername_";

        Event event = Event.newBuilder()
                           .setIdentifier(eventId)
                           .setName(myType)
                           .setPayload(ByteString.empty())
                           .putMetadata("Hello", "World")
                           .build();
        TaggedEvent taggedEvent = TaggedEvent.newBuilder()
                                             .setEvent(event)
                                             .addTag(Tag.newBuilder()
                                                        .setKey(ByteString.copyFrom(key.getBytes()))
                                                        .setValue(ByteString.copyFrom(myUsername.getBytes()))
                                                        .build())
                                             .build();
        AppendEventsResponse _unused = dcbEventChannel.startTransaction()
                                                      .append(taggedEvent)
                                                      .commit()
                                                      .join();

        Criterion typeCriterion = criterionWithOnlyType(myType);
        Criterion realTag = Criterion.newBuilder()
                                    .setTagsAndNames(
                                            TagsAndNamesCriterion.newBuilder()
                                                                 .addTag(Tag.newBuilder()
                                                                            .setKey(ByteString.copyFrom(key.getBytes()))
                                                                            .setValue(ByteString.copyFrom(myUsername.getBytes()))
                                                                            .build())
                                                    .build()
                                    ).build();
        ResultStream<SourceEventsResponse> responseStream = dcbEventChannel.source(SourceEventsRequest.newBuilder()
                                                                                                      .addCriterion(
                                                                                                              realTag)
                                                                                                      .build());
        SourceEventsResponse secondResponse = responseStream.nextIfAvailable(20, SECONDS);
        assertEquals(myType, secondResponse.getEvent().getEvent().getName()); // the event that matches criteria
        assertEquals(eventId, secondResponse.getEvent().getEvent().getIdentifier());
        SourceEventsResponse firstResponse = responseStream.nextIfAvailable(20, SECONDS);
        assertEquals(1, firstResponse.getConsistencyMarker()); // Consistency marker from onComplete
    }


    @Test
    void twoNonClashingAppends() {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        String eventId = "what";
        String myType = "myTypeThatIsNotTheSameAsInTheCriterion";

        Event taggedEvent = Event.newBuilder()
                                 .setName(myType)
                                 .setIdentifier(eventId)
                                 .build();


        TaggedEvent event = TaggedEvent.newBuilder()
                                       .setEvent(taggedEvent)
                                       .build();
        String key1 = "key1";
        String value1 = "value1";
        Criterion criterion1 = criterionWithOneTag(key1, value1);
        long consistencyMarker = 0L;
        ConsistencyCondition condition1 = ConsistencyCondition.newBuilder()
                                                              .setConsistencyMarker(consistencyMarker)
                                                              .addCriterion(criterion1)
                                                              .build();
        AppendEventsResponse response1 = dcbEventChannel.startTransaction()
                                                        .append(event)
                                                        .condition(condition1)
                                                        .commit()
                                                        .join();

        assertEquals(0L, response1.getLastPosition());

        Tag tag2 = Tag.newBuilder()
                      .setKey(ByteString.copyFromUtf8("key2"))
                      .setValue(ByteString.copyFromUtf8("value2"))
                      .build();
        Criterion criterion2 = Criterion.newBuilder()
                                        .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                                              .addName("name")
                                                                              .addTag(tag2)
                                                                              .build())
                                        .build();
        ConsistencyCondition condition2 = ConsistencyCondition.newBuilder()
                                                              .setConsistencyMarker(consistencyMarker)
                                                              .addCriterion(criterion2)
                                                              .build();
        AppendEventsResponse response2 = dcbEventChannel.startTransaction()
                                                        .append(event)
                                                        .condition(condition2)
                                                        .commit()
                                                        .join();

        assertEquals(1L, response2.getLastPosition());
    }

    private static @NotNull Criterion criterionWithOneTag(String key1, String value1) {
        Tag tag1 = Tag.newBuilder()
                      .setKey(ByteString.copyFromUtf8(key1))
                      .setValue(ByteString.copyFromUtf8(value1))
                      .build();
        Criterion criterion1 = Criterion.newBuilder()
                                        .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                                              .addName("name")
                                                                              .addTag(tag1)
                                                                              .build())
                                        .build();
        return criterion1;
    }

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


    private static @NotNull Criterion criterionWithOnlyType(String myType) {
        TagsAndNamesCriterion ttc = TagsAndNamesCriterion.newBuilder()
                                                         .addName(myType)
                                                         .build();
        Criterion typeCriterion = Criterion.newBuilder().setTagsAndNames(ttc).build();
        return typeCriterion;
    }
}
