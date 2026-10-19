/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Interceptor around a gRPC request to add header information as metadata to a message.
 *
 * @param <T> the key and value type of the header this interceptor will attach
 * @author Marc Gathier
 * @since 4.0
 */
public class HeaderAttachingInterceptor<T> implements ClientInterceptor {

    private final Metadata.Key<T> header;
    private final T value;

    /**
     * Constructs a {@link HeaderAttachingInterceptor} attaching metadata using the given {@code header} as key, paired
     * with the given {@code value}
     *
     * @param header the {@link Metadata.Key} to attach on intercepted messages
     * @param value  the object of type {@code T} to attach on intercepted messages
     */
    public HeaderAttachingInterceptor(Metadata.Key<T> header, T value) {
        this.header = header;
        this.value = value;
    }

    @Override
    public <REQ, RESP> ClientCall<REQ, RESP> interceptCall(MethodDescriptor<REQ, RESP> methodDescriptor,
                                                           CallOptions callOptions,
                                                           Channel channel) {
        ClientCall<REQ, RESP> call = channel.newCall(methodDescriptor, callOptions);
        return value == null ? call : new HeaderAttachedCall<>(call);
    }

    private class HeaderAttachedCall<REQ, RESP> extends ForwardingClientCall.SimpleForwardingClientCall<REQ, RESP> {

        public HeaderAttachedCall(ClientCall<REQ, RESP> call) {
            super(call);
        }

        @Override
        public void start(Listener<RESP> responseListener, Metadata headers) {
            headers.put(header, value);
            super.start(responseListener, headers);
        }
    }
}
