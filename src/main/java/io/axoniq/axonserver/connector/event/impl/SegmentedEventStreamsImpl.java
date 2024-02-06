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
import java.util.function.IntConsumer;

public class SegmentedEventStreamsImpl
        implements SegmentedEventStreams, ClientResponseObserver<StreamCommand, StreamSignal> {
    private static final Logger logger = LoggerFactory.getLogger(SegmentedEventStreamsImpl.class);
    private static final Runnable NO_OP = () -> {
    };

    private final Map<Integer, BufferedSegmentEventStream> openSegments = new ConcurrentHashMap<>();
    private final String streamId;
    private final String clientId;

    private ClientCallStreamObserver<StreamCommand> outboundStream;
    private final AtomicReference<Runnable> onAvailableCallback = new AtomicReference<>(NO_OP);
    private final Set<IntConsumer> onSegmentClosedCallbacks = new CopyOnWriteArraySet<>();
    private final Set<IntConsumer> onSegmentOpenedCallbacks = new CopyOnWriteArraySet<>();


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

        outboundStream.onNext(StreamCommand.newBuilder().setOpen(openRequest).build());
    }


    public void close() {
        outboundStream.onCompleted();
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<StreamCommand> clientCallStreamObserver) {
        outboundStream = clientCallStreamObserver;
    }

    @Override
    public void onNext(StreamSignal streamSignal) {
        BufferedSegmentEventStream segment;
        if (streamSignal.hasEvent()) {
            boolean isNew = !openSegments.containsKey(streamSignal.getSegment());
            segment = openSegments.computeIfAbsent(streamSignal.getSegment(),
                                                   s -> new BufferedSegmentEventStream(progress -> onProgress(s,
                                                                                                              progress)));
            segment.onNext(streamSignal.getEvent());
            if (isNew) {
                onSegmentOpenedCallbacks.forEach(callback -> callback.accept(streamSignal.getSegment()));
            }
            onAvailableCallback.get().run();
        }
        if (streamSignal.getClosed()) {
            segment = openSegments.remove(streamSignal.getSegment());
            if (segment != null) {
                segment.onCompleted();
                onSegmentClosedCallbacks.forEach(callback -> callback.accept(streamSignal.getSegment()));
            }
        }
    }

    private void onProgress(int segment, long progress) {
        synchronized (outboundStream) {
            outboundStream.onNext(StreamCommand.newBuilder()
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
            outboundStream.onCompleted();
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
            onSegmentClosedCallbacks.forEach(callback -> callback.accept(segment));
        });
        onAvailableCallback.get().run();
    }

    @Override
    public void onSegmentClosed(IntConsumer callback) {
        onSegmentClosedCallbacks.add(callback);
    }

    @Override
    public void onSegmentOpened(IntConsumer callback) {
        onSegmentOpenedCallbacks.add(callback);
    }

    @Override
    public SegmentEventStream segment(int segment) {
        return openSegments.get(segment);
    }
}
