package io.axoniq.axonserver.connector.impl;

import io.grpc.Metadata;

public class Headers {

    public static final Metadata.Key<String> CONTEXT =
            Metadata.Key.of("AxonIQ-Context", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> ACCESS_TOKEN =
            Metadata.Key.of("AxonIQ-Access-Token", Metadata.ASCII_STRING_MARSHALLER);


    private Headers() {
    }
}
