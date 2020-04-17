package io.axoniq.axonserver.connector.impl;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

public class DisconnectedChannel extends ManagedChannel {
    @Override
    public ManagedChannel shutdown() {
        return this;
    }

    @Override
    public boolean isShutdown() {
        return true;
    }

    @Override
    public boolean isTerminated() {
        return true;
    }

    @Override
    public ManagedChannel shutdownNow() {
        return this;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        return new ClientCall<RequestT, ResponseT>() {
            @Override
            public void start(Listener<ResponseT> responseListener, Metadata headers) {
                responseListener.onClose(Status.UNAVAILABLE, null);
            }

            @Override
            public void request(int numMessages) {
                // ignore
            }

            @Override
            public void cancel(@Nullable String message, @Nullable Throwable cause) {
                // great
            }

            @Override
            public void halfClose() {
                // great
            }

            @Override
            public void sendMessage(RequestT message) {
                throw new StatusRuntimeException(Status.UNAVAILABLE);
            }
        };
    }

    @Override
    public String authority() {
        return "unknown";
    }

    @Override
    public ConnectivityState getState(boolean requestConnection) {
        return ConnectivityState.SHUTDOWN;
    }

    @Override
    public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {
        if (source != ConnectivityState.SHUTDOWN) {
            callback.run();
        }
    }

}
