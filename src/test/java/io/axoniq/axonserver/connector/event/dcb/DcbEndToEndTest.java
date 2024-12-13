package io.axoniq.axonserver.connector.event.dcb;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.grpc.event.dcb.AppendRequest;
import io.axoniq.axonserver.grpc.event.dcb.AppendResponse;
import io.axoniq.axonserver.grpc.event.dcb.ConsistencyCondition;
import io.axoniq.axonserver.grpc.event.dcb.Criterion;
import io.axoniq.axonserver.grpc.event.dcb.IdentifiableEventPayload;
import io.axoniq.axonserver.grpc.event.dcb.Position;
import io.axoniq.axonserver.grpc.event.dcb.SourceRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceResponse;
import io.axoniq.axonserver.grpc.event.dcb.Tag;
import io.axoniq.axonserver.grpc.event.dcb.TaggedEventPayload;
import io.axoniq.axonserver.grpc.event.dcb.TagsAndTypesCriterion;
import io.axoniq.axonserver.grpc.event.dcb.TypedEventPayload;
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
    void noConditionAppend() throws InterruptedException {
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
        axonServerContainer.followOutput(logConsumer);



        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        String eventId = "what";
        String myType = "myUniqueTypeNobodyElseWillUse" + UUID.randomUUID();

        TypedEventPayload typedEventPayload = typedEventPayload(eventId, myType);
        TaggedEventPayload taggedEvent = TaggedEventPayload.newBuilder()
                                                           .setTypedEvent(typedEventPayload)
                                                           .build();
        AppendResponse _unused = dcbEventChannel.startTransaction()
                                                .append(AppendRequest.Event.newBuilder().setEvent(taggedEvent).build())
                                                .commit()
                                                .join();

        Criterion typeCriterion = criterionWithOnlyType(myType);
        SourceResponse sourceResponse = dcbEventChannel.source(SourceRequest.newBuilder()
                                                                            .addCriterion(typeCriterion)
                                                                            .build())
                                                       .nextIfAvailable(1, SECONDS);
        TypedEventPayload typedEvent = sourceResponse.getEvent().getTaggedEvent().getTypedEvent();
        assertEquals(myType, typedEvent.getType());
        assertEquals(eventId, typedEvent.getEvent().getIdentifier());
    }


    @Test
    void twoNonClashingAppends() {
        DcbEventChannel dcbEventChannel = connection.dcbEventChannel();
        String eventId = "what";
        String myType = "myTypeThatIsNotTheSameAsInTheCriterion";

        TypedEventPayload typedEventPayload = typedEventPayload(eventId, myType);

        TaggedEventPayload taggedEvent = TaggedEventPayload.newBuilder()
                                                           .setTypedEvent(typedEventPayload)
                                                           .build();


        AppendRequest.Event event = AppendRequest.Event.newBuilder()
                                                       .setEvent(taggedEvent)
                                                       .build();
        String key1 = "key1";
        String value1 = "value1";
        Criterion criterion1 = criterionWithOneTag(key1, value1);
        Position consistencyMarker = Position.newBuilder()
                                             .setSequence(0L)
                                             .build();
        ConsistencyCondition condition1 = ConsistencyCondition.newBuilder()
                                                              .setConsistencyMarker(consistencyMarker)
                                                              .addCriterion(criterion1)
                                                              .build();
        AppendResponse response1 = dcbEventChannel.startTransaction()
                                                  .append(event)
                                                  .condition(condition1)
                                                  .commit()
                                                  .join();

        assertEquals(0L, response1.getLastPosition().getSequence());

        Tag tag2 = Tag.newBuilder()
                      .setKey(ByteString.copyFromUtf8("key2"))
                      .setValue(ByteString.copyFromUtf8("value2"))
                      .build();
        Criterion criterion2 = Criterion.newBuilder()
                                        .setTagsAndTypes(TagsAndTypesCriterion.newBuilder()
                                                                              .addType("type")
                                                                              .addTag(tag2)
                                                                              .build())
                                        .build();
        ConsistencyCondition condition2 = ConsistencyCondition.newBuilder()
                                                              .setConsistencyMarker(consistencyMarker)
                                                              .addCriterion(criterion2)
                                                              .build();
        AppendResponse response2 = dcbEventChannel.startTransaction()
                                                  .append(event)
                                                  .condition(condition2)
                                                  .commit()
                                                  .join();

        assertEquals(1L, response2.getLastPosition().getSequence());


    }

    private static @NotNull Criterion criterionWithOneTag(String key1, String value1) {
        Tag tag1 = Tag.newBuilder()
                      .setKey(ByteString.copyFromUtf8(key1))
                      .setValue(ByteString.copyFromUtf8(value1))
                      .build();
        Criterion criterion1 = Criterion.newBuilder()
                                        .setTagsAndTypes(TagsAndTypesCriterion.newBuilder()
                                                                              .addType("type")
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
        TagsAndTypesCriterion ttc = TagsAndTypesCriterion.newBuilder()
                                                         .addType(myType)
                                                         .build();
        Criterion typeCriterion = Criterion.newBuilder().setTagsAndTypes(ttc).build();
        return typeCriterion;
    }

    private static @NotNull TypedEventPayload typedEventPayload(String eventId, String myType) {
        IdentifiableEventPayload payload = identifiableEventPayload(eventId);
        TypedEventPayload typedEventPayload = TypedEventPayload.newBuilder()
                                                               .setType(myType)
                                                               .setEvent(payload)
                                                               .build();
        return typedEventPayload;
    }

    private static @NotNull IdentifiableEventPayload identifiableEventPayload(String eventId) {
        IdentifiableEventPayload payload = IdentifiableEventPayload.newBuilder()
                                                                   .setIdentifier(eventId)
                                                                   .setPayload(ByteString.empty())
                                                                   .build();
        return payload;
    }
}
