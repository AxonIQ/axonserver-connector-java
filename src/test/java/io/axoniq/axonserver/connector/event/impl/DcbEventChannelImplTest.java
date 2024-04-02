/*
 * Copyright (c) 2020-2024. AxonIQ
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

package io.axoniq.axonserver.connector.event.impl;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.ResultStreamPublisher;
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.ConsistencyCondition;
import io.axoniq.axonserver.grpc.event.dcb.Criterion;
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.Position;
import io.axoniq.axonserver.grpc.event.dcb.ServerSentEvent;
import io.axoniq.axonserver.grpc.event.dcb.StreamQuery;
import io.axoniq.axonserver.grpc.event.dcb.Tag;
import io.axoniq.axonserver.grpc.event.dcb.TaggedEvent;
import io.axoniq.axonserver.grpc.event.dcb.TagsAndTypeCriterion;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

class DcbEventChannelImplTest {

    private AxonServerConnection connection;
    private AxonServerConnectionFactory client;

    @BeforeEach
    void setup() {
        client = AxonServerConnectionFactory.forClient("dcb")
                                            .build();
        connection = client.connect("default");
    }

    @AfterEach
    void teardown() {
        connection.disconnect();
        client.shutdown();
    }

    @Test
    void append() throws ExecutionException, InterruptedException {
        String type = "event-type";
        Tag tag = Tag.newBuilder()
                     .setKey(ByteString.copyFromUtf8("aggregateId"))
                     .setValue(ByteString.copyFromUtf8("my-aggregate"))
                     .build();
//        Tag tag1 = Tag.newBuilder()
//                     .setKey(ByteString.copyFromUtf8("student"))
//                     .setValue(ByteString.copyFromUtf8("marco"))
//                     .build();
        TagsAndTypeCriterion tagsAndTypes = TagsAndTypeCriterion.newBuilder()
                                                                .setType(type)
                                                                .addTag(tag)
                                                                .build();
        Criterion criterion = Criterion.newBuilder()
                                       .setFilterKey("filter-key")
                                       .setTagsAndTypes(tagsAndTypes)
                                       .build();
        StreamQuery streamQuery = StreamQuery.newBuilder()
                                             .addCriterion(criterion)
                                             .build();
        Position consistencyMarker = Position.newBuilder()
                                             .setSequence(0L)
                                             .build();
        ConsistencyCondition consistencyCondition = ConsistencyCondition.newBuilder()
                                                                        .setConsistencyMarker(consistencyMarker)
                                                                        .setStreamQuery(streamQuery)
                                                                        .build();
        Event event = Event.newBuilder()
                           .setIdentifier(UUID.randomUUID().toString())
                           .setPayload(ByteString.copyFromUtf8("payload"))
                           .build();
        TaggedEvent taggedEvent = TaggedEvent.newBuilder()
                                             .setType(type)
                                             .addTag(tag)
//                .addTag(tag1)
                                             .setEvent(event)
                                             .build();
        AppendEventsResponse appendEventsResponse =
                connection.dcbEventChannel()
                          .startTransaction()
                          .condition(consistencyCondition)
                          .append(taggedEvent)
                          .commit()
                          .get();
        System.out.println("last position: " + appendEventsResponse.getLastPosition());
    }

    @Test
    void events() {
        ResultStream<ServerSentEvent> events = connection.dcbEventChannel().events(StreamQuery.newBuilder().build());
        Flux.from(new ResultStreamPublisher<>(() -> events))
            .doOnError(t -> t.printStackTrace())
            .subscribe(sse -> System.out.println(sse));
    }
}
