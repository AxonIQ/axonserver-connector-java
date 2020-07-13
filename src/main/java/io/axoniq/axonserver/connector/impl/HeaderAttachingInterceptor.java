/*
 * Copyright (c) 2020. AxonIQ
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

package io.axoniq.axonserver.connector.impl;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Interceptor around a gRPC request to add a Context element to the metadata.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class HeaderAttachingInterceptor<T> implements ClientInterceptor {

    private final Metadata.Key<T> header;
    private final T value;

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
