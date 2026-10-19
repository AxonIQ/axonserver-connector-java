/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.demo;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.query.QueryChannel;
import io.axoniq.axonserver.connector.query.QueryDefinition;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.ProcessingKey;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;

import java.util.Scanner;

public class ConnectorRunner_Query {

    public static class QuerySender {

        public static void main(String[] args) {


            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient")
                                                                                 .build();
            AxonServerConnection contextConnection = testSubject.connect("default");

            QueryChannel channel = contextConnection.queryChannel();
            ResultStream<QueryResponse> result = channel.query(QueryRequest.newBuilder()
                                                                           .addProcessingInstructions(ProcessingInstruction.newBuilder().setKey(ProcessingKey.NR_OF_RESULTS).setValue(MetaDataValue.newBuilder().setNumberValue(20).build()).build())
                                                                           .setQuery("java.lang.String")
                                                                           .setPayload(SerializedObject.newBuilder().setType("java.lang.String").setData(ByteString.copyFromUtf8("Hello world")).build()).build());

            while (!result.isClosed()) {
                QueryResponse response = result.nextIfAvailable();
                if(response != null) {
                    System.out.println("Got response: " + response.getPayload().getData().toStringUtf8());
                }
            }
            result.close();
        }
    }

    public static class QueryHandler {

        public static void main(String[] args) {
            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient-Success")
                                                                                 .build();
            AxonServerConnection contextConnection = testSubject.connect("default");

            QueryChannel channel = contextConnection.queryChannel();
            channel.registerQueryHandler((q, r) -> {
                System.out.println("Handled query");
                r.sendLast(QueryResponse.newBuilder().setRequestIdentifier(q.getMessageIdentifier()).setPayload(q.getPayload()).build());
            }, new QueryDefinition(String.class.getName(), String.class));

            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();

            contextConnection.disconnect();
        }
    }

    public static class FaultyQueryHandler {

        public static void main(String[] args) {
            AxonServerConnectionFactory testSubject = AxonServerConnectionFactory.forClient("testClient-Faulty")
                                                                                 .build();
            AxonServerConnection contextConnection = testSubject.connect("default");

            QueryChannel channel = contextConnection.queryChannel();
            channel.registerQueryHandler((q, r) -> {
                                             System.out.println("Handled query");
                                             r.complete();
                                         }, new QueryDefinition(String.class.getName(), String.class),
                                         new QueryDefinition(String.class.getName(), Exception.class));

            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
        }
    }
}