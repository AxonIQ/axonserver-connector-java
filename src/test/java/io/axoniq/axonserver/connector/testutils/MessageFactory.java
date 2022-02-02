/*
 * Copyright (c) 2020-2021. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.testutils;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;

import java.util.UUID;

/**
 * Utility class to construct messages, like an {@link Event}.
 *
 * @author Allard Buijze
 */
public class MessageFactory {

    public static Event createEvent(String payload) {
        return createEvent(payload, System.currentTimeMillis());
    }

    public static Event createEvent(String payload, long timestamp) {
        return Event.newBuilder().setPayload(SerializedObject.newBuilder()
                                                             .setData(ByteString.copyFromUtf8(payload))
                                                             .setType("string"))
                    .setMessageIdentifier(UUID.randomUUID().toString())
                    .setTimestamp(timestamp)
                    .build();
    }

    public static Event createDomainEvent(String payload, String aggregateIdentifier, long sequence) {
        return Event.newBuilder().setPayload(SerializedObject.newBuilder()
                                                             .setData(ByteString.copyFromUtf8(payload))
                                                             .setType("string"))
                    .setAggregateType("Aggregate")
                    .setAggregateIdentifier(aggregateIdentifier)
                    .setAggregateSequenceNumber(sequence)
                    .setMessageIdentifier(UUID.randomUUID().toString())
                    .setTimestamp(System.currentTimeMillis())
                    .build();
    }
}
