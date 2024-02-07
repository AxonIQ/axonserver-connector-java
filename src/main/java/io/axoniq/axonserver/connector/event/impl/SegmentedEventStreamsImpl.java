package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.event.SegmentEventStream;
import io.axoniq.axonserver.connector.event.SegmentedEventStreams;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.streams.InitializationProperties;
import io.axoniq.axonserver.grpc.streams.OpenRequest;
import io.axoniq.axonserver.grpc.streams.ProgressRequest;
import io.axoniq.axonserver.grpc.streams.StreamCommand;
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

public class SegmentedEventStreamsImpl
        implements SegmentedEventStreams, ClientResponseObserver<StreamCommand, StreamSignal> {
    private static final Logger logger = LoggerFactory.getLogger(SegmentedEventStreamsImpl.class);
    private static final Consumer<Throwable> NO_OP = ex -> {
    };

    private final Map<Integer, BufferedSegmentEventStream> openSegments = new ConcurrentHashMap<>();
    private final String streamId;
    private final String clientId;

    private final AtomicReference<ClientCallStreamObserver<StreamCommand>> outboundStreamHolder = new AtomicReference<>();
    private final AtomicReference<Consumer<Throwable>> onClosedCallback = new AtomicReference<>(NO_OP);
    private final Set<Consumer<SegmentEventStream>> onSegmentOpenedCallbacks = new CopyOnWriteArraySet<>();


    public SegmentedEventStreamsImpl(ClientIdentification clientId, String streamId) {
        this.streamId = streamId;
        this.clientId = clientId.getClientId();
    }

    public void openConnection() {
        openConnection(null);
    }

    public void openConnection(InitializationProperties initializationProperties) {
        OpenRequest.Builder openRequest = OpenRequest.newBuilder()
                                                     .setStreamId(streamId)
                                                     .setClientId(clientId);

        if (initializationProperties != null) {
            openRequest.setInitializationProperties(initializationProperties);
        }

        outboundStreamHolder.get().onNext(StreamCommand.newBuilder().setOpen(openRequest).build());
    }


    public void close() {
        outboundStreamHolder.get().onCompleted();
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<StreamCommand> clientCallStreamObserver) {
        outboundStreamHolder.set(clientCallStreamObserver);
    }

    @Override
    public void onNext(StreamSignal streamSignal) {
        if (streamSignal.hasEvent()) {
            boolean isNew = !openSegments.containsKey(streamSignal.getSegment());
            BufferedSegmentEventStream segment = openSegments.computeIfAbsent(streamSignal.getSegment(),
                                                   s -> new BufferedSegmentEventStream(streamSignal.getSegment(), progress -> onProgress(s,
                                                                                                              progress)));
            segment.onNext(streamSignal.getEvent());
            if (isNew) {
                onSegmentOpenedCallbacks.forEach(callback -> callback.accept(segment));
            }
        }
        if (streamSignal.getClosed()) {
            BufferedSegmentEventStream segment = openSegments.remove(streamSignal.getSegment());
            if (segment != null) {
                segment.onCompleted();
            }
        }
    }

    private void onProgress(int segment, long progress) {
        synchronized (outboundStreamHolder) {
            outboundStreamHolder.get().onNext(StreamCommand.newBuilder()
                                               .setProgress(ProgressRequest.newBuilder()
                                                                           .setSegment(segment)
                                                                           .setPosition(progress)
                                                                           .build())
                                               .build());
        }
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

    @Override
    public void onSegmentOpened(Consumer<SegmentEventStream>  callback) {
        onSegmentOpenedCallbacks.add(callback);
    }

    @Override
    public void onClosed(Consumer<Throwable> closedCallback) {
        if (closedCallback == null ) {
            onClosedCallback.set(NO_OP);
        } else {
            onClosedCallback.set(closedCallback);
        }
    }
}
