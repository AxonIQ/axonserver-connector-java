/*
 * Copyright (c) 2020-2024. AxonIQ
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
package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.event.PersistentStream;
import io.axoniq.axonserver.connector.event.PersistentStreamCallbacks;
import io.axoniq.axonserver.connector.event.PersistentStreamSegment;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.streams.InitializationProperties;
import io.axoniq.axonserver.grpc.streams.Open;
import io.axoniq.axonserver.grpc.streams.ProgressAcknowledgement;
import io.axoniq.axonserver.grpc.streams.StreamRequest;
import io.axoniq.axonserver.grpc.streams.StreamSignal;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import javax.annotation.Nullable;

public class PersistentStreamImpl
        implements PersistentStream, ClientResponseObserver<StreamRequest, StreamSignal> {

    private static final Logger logger = LoggerFactory.getLogger(PersistentStreamImpl.class);
    private static final Consumer<Throwable> NO_OP = ex -> {
    };

    private final Map<Integer, BufferedPersistentStreamSegment> openSegments = new ConcurrentHashMap<>();
    private final String streamId;
    private final String clientId;

    private final AtomicReference<ClientCallStreamObserver<StreamRequest>> outboundStreamHolder = new AtomicReference<>();
    private final AtomicReference<Consumer<Throwable>> onClosedCallback = new AtomicReference<>(NO_OP);
    private final Set<Consumer<PersistentStreamSegment>> onSegmentOpenedCallbacks = new CopyOnWriteArraySet<>();
    private final Set<IntConsumer> segmentOnAvailable = new CopyOnWriteArraySet<>();
    private final Set<IntConsumer> segmentOnClose = new CopyOnWriteArraySet<>();

    private final int bufferSize;
    private final int refillBatch;

    public PersistentStreamImpl(ClientIdentification clientId, String streamId, int bufferSize, int refillBatch,
                                PersistentStreamCallbacks callbacks) {
        this.streamId = streamId;
        this.clientId = clientId.getClientId();
        this.bufferSize = bufferSize;
        this.refillBatch = refillBatch;
        if (callbacks.onAvailable() != null) {
            segmentOnAvailable.add(callbacks.onAvailable());
        }
        if (callbacks.onClosed() != null) {
            onClosedCallback.set(callbacks.onClosed());
        }
        if (callbacks.onSegmentOpened() != null) {
            onSegmentOpenedCallbacks.add(callbacks.onSegmentOpened());
        }
        if (callbacks.onSegmentClosed() != null) {
            segmentOnClose.add(callbacks.onSegmentClosed());
        }
    }

    public void openConnection() {
        openConnection(null);
    }

    public void openConnection(InitializationProperties initializationProperties) {
        Open.Builder openRequest = Open.newBuilder()
                                       .setStreamId(streamId)
                                       .setClientId(clientId);

        if (initializationProperties != null) {
            openRequest.setInitializationProperties(initializationProperties);
        }
        outboundStreamHolder.get().onNext(StreamRequest.newBuilder().setOpen(openRequest.build()).build());
    }

    public void close() {
        outboundStreamHolder.get().onCompleted();
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<StreamRequest> clientCallStreamObserver) {
        outboundStreamHolder.set(new StreamRequestClientCallStreamObserver(clientCallStreamObserver));
    }

    @Override
    public void onNext(StreamSignal streamSignal) {
        if (streamSignal.hasEvent()) {
            boolean isNew = !openSegments.containsKey(streamSignal.getSegment());
            BufferedPersistentStreamSegment segment = openSegments.computeIfAbsent(streamSignal.getSegment(),
                                                                                   s -> {
                                                                                       BufferedPersistentStreamSegment stream = new BufferedPersistentStreamSegment(
                                                                                               streamSignal.getSegment(),
                                                                                               bufferSize,
                                                                                               refillBatch,
                                                                                               progress -> acknowledge(s,
                                                                                                                       progress));
                                                                                       stream.beforeStart(
                                                                                               outboundStreamHolder.get());
                                                                                       stream.enableFlowControl();
                                                                                       return stream;
                                                                                   });
            if (isNew) {
                onSegmentOpenedCallbacks.forEach(callback -> callback.accept(segment));
                segmentOnAvailable.forEach(a -> segment.onAvailable(() -> a.accept(segment.segment())));
                segmentOnClose.forEach(a -> segment.onSegmentClosed(() -> a.accept(segment.segment())));
            }
            segment.onNext(streamSignal.getEvent());
        }
        if (streamSignal.getClosed()) {
            BufferedPersistentStreamSegment segment = openSegments.remove(streamSignal.getSegment());
            if (segment != null) {
                segment.onCompleted();
            }
        }
    }

    private void acknowledge(int segment, long progress) {
        outboundStreamHolder.get().onNext(StreamRequest.newBuilder()
                                                       .setAcknowledgeProgress(ProgressAcknowledgement.newBuilder()
                                                                                                      .setSegment(
                                                                                                              segment)
                                                                                                      .setPosition(
                                                                                                              progress)
                                                                                                      .build())
                                                       .build());
    }

    @Override
    public void onError(Throwable throwable) {
        logger.warn("Exception on stream {}", streamId, throwable);
        close(throwable);
    }

    @Override
    public void onCompleted() {
        try {
            outboundStreamHolder.get().onCompleted();
        } catch (Exception ex) {
            // Ignore exception
        }
        close(null);
    }

    private void close(Throwable throwable) {
        openSegments.forEach((segment, buffer) -> {
            if (throwable != null) {
                buffer.onError(throwable);
            } else {
                buffer.onCompleted();
            }
        });
        onClosedCallback.get().accept(throwable);
    }

    private class StreamRequestClientCallStreamObserver extends ClientCallStreamObserver<StreamRequest> {

        private final ClientCallStreamObserver<StreamRequest> clientCallStreamObserver;

        public StreamRequestClientCallStreamObserver(ClientCallStreamObserver<StreamRequest> clientCallStreamObserver) {
            this.clientCallStreamObserver = clientCallStreamObserver;
        }

        @Override
        public void cancel(@Nullable String s, @Nullable Throwable throwable) {
            clientCallStreamObserver.cancel(s, throwable);
        }

        @Override
        public boolean isReady() {
            return clientCallStreamObserver.isReady();
        }

        @Override
        public void setOnReadyHandler(Runnable runnable) {
            clientCallStreamObserver.setOnReadyHandler(runnable);
        }

        @Override
        public void request(int i) {
            clientCallStreamObserver.request(i);
        }

        @Override
        public void setMessageCompression(boolean b) {
            clientCallStreamObserver.setMessageCompression(b);
        }

        @Override
        public void disableAutoInboundFlowControl() {
            clientCallStreamObserver.disableAutoInboundFlowControl();
        }

        @Override
        public void disableAutoRequestWithInitial(int request) {
            // all assigned segments for a persistent stream use the same stream observer
        }

        @Override
        public void onNext(StreamRequest streamRequest) {
            synchronized (outboundStreamHolder) {
                clientCallStreamObserver.onNext(streamRequest);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            clientCallStreamObserver.onError(throwable);
        }

        @Override
        public void onCompleted() {
            clientCallStreamObserver.onCompleted();
        }
    }
}
