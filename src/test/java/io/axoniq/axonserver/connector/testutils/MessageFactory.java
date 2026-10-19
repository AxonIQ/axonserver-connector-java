/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
