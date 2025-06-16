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

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.event.PersistentStream;
import io.axoniq.axonserver.connector.event.PersistentStreamCallbacks;
import io.axoniq.axonserver.connector.event.PersistentStreamSegment;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.streams.InitializationProperties;
import io.axoniq.axonserver.grpc.streams.Open;
import io.axoniq.axonserver.grpc.streams.ProgressAcknowledgement;
import io.axoniq.axonserver.grpc.streams.SegmentError;
import io.axoniq.axonserver.grpc.streams.StreamRequest;
import io.axoniq.axonserver.grpc.streams.StreamSignal;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;

/**
 * Implementation of the {@link PersistentStream}.
 */
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
    private final Set<Consumer<PersistentStreamSegment>> segmentOnAvailable = new CopyOnWriteArraySet<>();
    private final Set<Consumer<PersistentStreamSegment>> segmentOnClose = new CopyOnWriteArraySet<>();
    private final Set<Integer> closeConfirmationsSent = new CopyOnWriteArraySet<>();

    private final AtomicBoolean closed = new AtomicBoolean();
    private final int bufferSize;
    private final int refillBatch;

    /**
     * Constructs a {@link PersistentStreamImpl}.
     *
     * @param clientId    the identification of the client
     * @param streamId    the identifier for the persistent stream
     * @param bufferSize  the number of events to buffer locally
     * @param refillBatch the number of events to be consumed prior to refilling the buffer
     * @param callbacks   the callbacks that are invoked on persistent stream events
     */
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

    /**
     * Sends an open request to Axon Server to start a persistent stream connection. The connection is closed with an
     * error if the persistent stream does not exist.
     */
    public void openConnection() {
        openConnection(null);
    }

    /**
     * Sends an open request to Axon Server to start a persistent stream connection. If {@code initializationProperties}
     * are provided these are added to the request to create the persistent stream if it does not exist.
     *
     * @param initializationProperties optional properties to create the persistent stream
     */
    public void openConnection(InitializationProperties initializationProperties) {
        Open.Builder openRequest = Open.newBuilder()
                                       .setStreamId(streamId)
                                       .setClientId(clientId);

        if (initializationProperties != null) {
            openRequest.setInitializationProperties(initializationProperties);
        }
        outboundStreamHolder.get().onNext(StreamRequest.newBuilder().setOpen(openRequest.build()).build());
    }

    /**
     * Close this stream and signal downstream consumer that the stream is closed "erroneously" in order to trigger
     * reconnect mechanisms. This is to ensure that consumers of this stream can distinguish between regularly closed
     * streams, and those closed with the intent to re-establish a connection.
     */
    public void triggerReconnect() {
        if (closed.get()) {
            logger.info("{}: Already closed, cannot trigger reconnect", streamId);
            return;
        }
        // first, close gracefully
        close();
        AxonServerException reconnectRequested = new AxonServerException(ErrorCategory.OTHER,
                                                                         "Client initiated reconnect",
                                                                         "client");
        // notify clients that the connection "failed" and should be reconnected
        onClosedCallback.get().accept(reconnectRequested);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            Instant timeout = Instant.now().plus(Duration.ofSeconds(2));
            openSegments.forEach((segmentNumber, segment) -> segment.close());
            while (closeConfirmationsSent.size() != openSegments.size() && Instant.now().isBefore(timeout)) {
                try {
                    logger.debug("{}: Waiting for segments to complete {} of {}",
                                 streamId,
                                 closeConfirmationsSent.size(),
                                 openSegments.size());
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            logger.debug("{}: Waited for segments to complete {}", streamId, closeConfirmationsSent);
            sendCompleted();
        }
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<StreamRequest> clientCallStreamObserver) {
        outboundStreamHolder.set(new StreamRequestClientCallStreamObserver(clientCallStreamObserver));
    }

    @Override
    public void onNext(StreamSignal streamSignal) {
        if (streamSignal.hasOpen()) {
            getPersistentStreamSegment(streamSignal.getSegment());
        }
        if (streamSignal.hasEvent()) {
            BufferedPersistentStreamSegment segment = getPersistentStreamSegment(streamSignal.getSegment());
            segment.onNext(streamSignal.getEvent());
        }
        if (streamSignal.getClosed()) {
            logger.debug("Received: {} - closed", streamSignal.getSegment());
            BufferedPersistentStreamSegment segment = openSegments.remove(streamSignal.getSegment());
            if (segment != null) {
                segment.onCompleted();
            }
        }
    }

    private BufferedPersistentStreamSegment getPersistentStreamSegment(int segmentNr) {
        boolean isNew = !openSegments.containsKey(segmentNr);
        BufferedPersistentStreamSegment segment =
                openSegments.computeIfAbsent(segmentNr,
                                             s -> {
                                                 BufferedPersistentStreamSegment stream = new BufferedPersistentStreamSegment(
                                                         streamId,
                                                         segmentNr,
                                                         bufferSize,
                                                         refillBatch,
                                                         progress -> acknowledge(s, progress),
                                                         error -> sendError(s, error));
                                                 stream.beforeStart(outboundStreamHolder.get());
                                                 stream.enableFlowControl();
                                                 return stream;
                                             });
        if (isNew) {
            onSegmentOpenedCallbacks.forEach(callback -> callback.accept(segment));
            segmentOnAvailable.forEach(a -> segment.onAvailable(() -> a.accept(segment)));
            segmentOnClose.forEach(a -> segment.onSegmentClosed(() -> a.accept(segment)));
            closeConfirmationsSent.remove(segment.segment());
        }
        return segment;
    }

    private void acknowledge(int segment, long progress) {
        try {
            doIfNotNull(outboundStreamHolder.get(), call -> call.onNext(
                    StreamRequest.newBuilder()
                                 .setAcknowledgeProgress(ProgressAcknowledgement.newBuilder()
                                                                                .setSegment(segment)
                                                                                .setPosition(progress)
                                                                                .build())
                                 .build()));
        } catch (Exception e) {
            logger.debug("Failed to send acknowledgement.", e);
        }
        if (progress == PersistentStreamSegment.PENDING_WORK_DONE_MARKER) {
            logger.info("{}: Close confirmed for segment {}", streamId, segment);
            closeConfirmationsSent.add(segment);
        }
    }

    private void sendError(int segment, String error) {
        doIfNotNull(outboundStreamHolder.get(),
                    osh -> osh.onNext(StreamRequest.newBuilder()
                                                   .setError(SegmentError.newBuilder()
                                                                         .setSegment(segment)
                                                                         .setError(error)
                                                                         .build())
                                                   .build()));
    }

    @Override
    public void onError(Throwable throwable) {
        logger.warn("{}: Error on stream: {}", streamId, throwable.getMessage(), throwable);
        close(throwable);
    }

    @Override
    public void onCompleted() {
        sendCompleted();
        close(null);
    }

    private void sendCompleted() {
        try {
            doIfNotNull(outboundStreamHolder.getAndSet(null), StreamObserver::onCompleted);
        } catch (Exception ex) {
            // Ignore exception
        }
    }

    private void close(Throwable throwable) {
        boolean wasClosed = closed.getAndSet(true);
        if (wasClosed) {
            logger.info("{}: Already closed, cannot close again", streamId);
            return;
        }
        openSegments.forEach((segment, buffer) -> {
            try {
                if (throwable != null) {
                    buffer.onError(throwable);
                } else {
                    buffer.onCompleted();
                }
            } catch (Exception ex) {
                logger.debug("{}: Exception while completing segment {}", streamId, buffer.segment(), ex);
            }
        });
        onClosedCallback.get().accept(throwable);
    }

    /**
     * Wrapper for the provided ClientCallStreamObserver. As a persistent stream uses one StreamObserver for all
     * segments, it should not cancel the stream observer when one of the segments is cancelled. Also, it uses flow
     * control per segment, so overriding the auto inbound flow control is disabled
     */
    private class StreamRequestClientCallStreamObserver extends ClientCallStreamObserver<StreamRequest> {

        private final ClientCallStreamObserver<StreamRequest> clientCallStreamObserver;

        public StreamRequestClientCallStreamObserver(ClientCallStreamObserver<StreamRequest> clientCallStreamObserver) {
            this.clientCallStreamObserver = clientCallStreamObserver;
        }

        @Override
        public void cancel(@Nullable String s, @Nullable Throwable throwable) {
            logger.debug("{}: Ignore cancel: {}", streamId, s, throwable);
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
            // all assigned segments for a persistent stream use the same stream observer
        }

        @Override
        public void disableAutoRequestWithInitial(int request) {
            // all assigned segments for a persistent stream use the same stream observer
        }

        @Override
        public void onNext(StreamRequest streamRequest) {
            // requests need to be synchronized as multiple segments use this stream observer
            synchronized (outboundStreamHolder) {
                logger.trace("Send {}", streamRequest);
                clientCallStreamObserver.onNext(streamRequest);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            try {
                clientCallStreamObserver.onError(throwable);
            } catch (IllegalStateException ex) {
                // ignore exceptions on error
            }
        }

        @Override
        public void onCompleted() {
            try {
                clientCallStreamObserver.onCompleted();
            } catch (IllegalStateException ex) {
                // ignore exceptions on close
            }
        }
    }
}
