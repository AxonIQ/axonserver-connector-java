package io.axoniq.axonserver.connector.testutils;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;

import java.util.UUID;

public class MessageFactory {
    public static Event createEvent(String payload) {
        return Event.newBuilder().setPayload(SerializedObject.newBuilder()
                                                             .setData(ByteString.copyFromUtf8(payload))
                                                             .setType("string"))
                    .setMessageIdentifier(UUID.randomUUID().toString())
                    .setTimestamp(System.currentTimeMillis())
                    .build();
    }
}
