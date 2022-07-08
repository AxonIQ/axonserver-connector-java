/*
 * Copyright (c) 2021. AxonIQ
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

import io.axoniq.axonserver.grpc.FlowControl;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.axonserver.connector.impl.AssertUtils.assertParameter;

/**
 * Abstract implementation of a {@link ClientResponseObserver} providing flow control.
 *
 * @param <IN>  the type of entries returned by this stream
 * @param <OUT> the type of message used for flow control in this stream
 */
public abstract class FlowControlledStream<IN, OUT> implements ClientResponseObserver<OUT, IN> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final AtomicInteger permitsConsumed = new AtomicInteger();
    private final String clientId;
    private final int permits;
    private final int refillBatch;
    private final FlowControl flowControl;
    private ClientCallStreamObserver<OUT> outboundStream;

    /**
     * Constructs a {@link FlowControlledStream}.
     *
     * @param clientId    the client identifier which initiated this stream
     * @param permits     the number of permits this stream should receive
     * @param refillBatch the number of permits to be consumed prior to requesting new permits
     */
    protected FlowControlledStream(String clientId, int permits, int refillBatch) {
        assertParameter(permits > 0, "Permits must be > 0");
        assertParameter(refillBatch <= permits, "The refillBatch must be smaller than the number of permits");
        assertParameter(clientId != null, "The clientId must not be null");

        this.clientId = clientId;
        this.permits = permits;
        this.refillBatch = refillBatch;
        flowControl = FlowControl.newBuilder()
                                 .setPermits(refillBatch)
                                 .setClientId(clientId)
                                 .build();
    }

    /**
     * Enables flow control for this stream. Will only set up flow control if the permits batch size is larger than
     * {@code 0}.
     */
    public void enableFlowControl() {
        if (refillBatch > 0) {
            permitsConsumed.set(0);
            OUT out = buildInitialFlowControlMessage(FlowControl.newBuilder()
                                                                .setPermits(permits)
                                                                .setClientId(clientId)
                                                                .build());
            if (out != null) {
                outboundStream().onNext(out);
            }
        }
    }

    /**
     * Build a flow control message of type {@code OUT} based on the given {@code flowControl}.
     *
     * @param flowControl the {@link FlowControl} message used to base this implementation's flow control message on
     * @return the message used by this implementation to request new entries of type {@code IN}
     */
    protected abstract OUT buildFlowControlMessage(FlowControl flowControl);

    /**
     * Builds the initial flow control message of type {@code OUT} based on the given {@code flowControl}.
     *
     * @param flowControl the {@link FlowControl} message used to base this implementation's flow control message on
     * @return the initial message used by this implementation to request new entries of type {@code IN}
     */
    protected OUT buildInitialFlowControlMessage(FlowControl flowControl) {
        return buildFlowControlMessage(flowControl);
    }

    /**
     * Return the client identifier which has initiated this stream.
     *
     * @return the client identifier which has initiated this stream.
     */
    protected String clientId() {
        return clientId;
    }

    protected int permits() {
        return permits;
    }

    protected int permitsConsumed() {
        return permitsConsumed.get();
    }

    /**
     * Notifier when an entry has been consumed from this stream. Keeps track of the number of permits which has been
     * consumed and will automatically ask for new permits if the {@code permitsBatch} size has been reached.
     */
    protected void markConsumed() {
        if (refillBatch > 0) {
            int ticker = permitsConsumed.updateAndGet(current -> {
                if (current == refillBatch - 1) {
                    return 0;
                }
                return current + 1;
            });
            if (ticker == 0) {
                OUT permitsRequest = buildFlowControlMessage(flowControl);
                logger.debug("Requesting additional {} permits", refillBatch);
                outboundStream.request(refillBatch);
                if (permitsRequest != null) {
                    outboundStream().onNext(permitsRequest);
                }
            }
        }
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<OUT> requestStream) {
        if (refillBatch > 0) {
            requestStream.disableAutoRequestWithInitial(permits);
        }
        this.outboundStream = requestStream;
    }

    /**
     * Return the {@link ClientCallStreamObserver} serving as the outbound stream. Can be used to send additional
     * messages required to tap into the flow control of this stream, or the complete the stream altogether.
     *
     * @return the {@link ClientCallStreamObserver} serving as the outbound stream
     */
    protected ClientCallStreamObserver<OUT> outboundStream() {
        return outboundStream;
    }
}
