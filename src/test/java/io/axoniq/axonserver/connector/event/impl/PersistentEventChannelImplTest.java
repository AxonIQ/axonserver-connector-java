package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStreamPublisher;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.PersistedStreamProperties;
import io.axoniq.axonserver.connector.event.PersistentStream;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.junit.jupiter.api.*;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class PersistentEventChannelImplTest {

    private static final Logger logger = LoggerFactory.getLogger(PersistentEventChannelImplTest.class);
    private EventChannel eventChannel;

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException {
        AxonServerConnectionFactory connectionFactory = AxonServerConnectionFactory.forClient("demo",
                                                                                              UUID.randomUUID()
                                                                                                  .toString()).build();
        eventChannel = connectionFactory.connect("second").eventChannel();
        eventChannel.deletePersistentStream("sample-events").get();
    }

    @Test
    @Disabled
    void openPersistedStream() throws ExecutionException, InterruptedException, TimeoutException {
        PersistentStream streams = eventChannel.openPersistentStream(
                "sample-events", new PersistedStreamProperties(
                        null,
                        2,
                        "AggregateIdentifier",
                        null,
                        0,
                        null));
        streams.onSegmentOpened(segmentEventStream -> {
            Flux.from(new ResultStreamPublisher<>(() -> segmentEventStream))
                .publishOn(Schedulers.newSingle("segment-" + segmentEventStream.segment()))
                .subscribe(new Subscriber<EventWithToken>() {
                    private Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription subscription) {
                        this.subscription = subscription;
                        subscription.request(100);
                    }

                    @Override
                    public void onNext(EventWithToken event) {
                        logger.info("{}: next available -> {} ", segmentEventStream.segment(), event);
                        segmentEventStream.acknowledge(event.getToken());
                        subscription.request(1);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.warn("{}: exception ", segmentEventStream.segment(), throwable);
                    }

                    @Override
                    public void onComplete() {
                        logger.info("{}: closed ", segmentEventStream.segment());
                    }
                });
        });

        eventChannel.appendEvents(Event.newBuilder()
                                       .setAggregateIdentifier("1234")
                                       .build()).get(1, TimeUnit.SECONDS);
        eventChannel.appendEvents(Event.newBuilder()
                                       .setAggregateIdentifier("1235")
                                       .build()).get(1, TimeUnit.SECONDS);
        eventChannel.appendEvents(Event.newBuilder()
                                       .setAggregateIdentifier("1236")
                                       .build()).get(1, TimeUnit.SECONDS);

        long last = eventChannel.getLastToken().get();
        Thread.sleep(2000);

        streams.close();
    }
}