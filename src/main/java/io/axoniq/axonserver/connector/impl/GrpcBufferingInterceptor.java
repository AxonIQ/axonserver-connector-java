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
 * Interceptor that immediately requests a number of messages from the server, to increase the flow of messages.
 * <p>
 * As an additional message is requested by default each time a message is received, setting an initial request amount
 * will allow that number of messages to be "in transit" before the server stops sending more.
 */
public class GrpcBufferingInterceptor implements ClientInterceptor {

    private final int additionalBuffer;

    /**
     * Initialize the interceptor to ask for {@code additionalBuffer} amount of messages from the server.
     *
     * @param additionalBuffer the number of messages the server may send before waiting for permits to be renewed
     */
    public GrpcBufferingInterceptor(int additionalBuffer) {
        this.additionalBuffer = additionalBuffer;
    }

    @Override
    public <REQ, RESP> ClientCall<REQ, RESP> interceptCall(MethodDescriptor<REQ, RESP> method,
                                                           CallOptions callOptions,
                                                           Channel next) {
        ClientCall<REQ, RESP> call = next.newCall(method, callOptions);
        if (additionalBuffer == 0 || method.getType().serverSendsOneMessage()) {
            return call;
        }
        return new AdditionalMessageRequestingCall<>(call, additionalBuffer);
    }

    private static class AdditionalMessageRequestingCall<REQ, RESP>
            extends ForwardingClientCall.SimpleForwardingClientCall<REQ, RESP> {

        private final int additionalBuffer;

        public AdditionalMessageRequestingCall(ClientCall<REQ, RESP> call, int additionalBuffer) {
            super(call);
            this.additionalBuffer = additionalBuffer;
        }

        @Override
        public void start(Listener<RESP> responseListener, Metadata headers) {
            super.start(responseListener, headers);
            request(additionalBuffer);
        }
    }
}
