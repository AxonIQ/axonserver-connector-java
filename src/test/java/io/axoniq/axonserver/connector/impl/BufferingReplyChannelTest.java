/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.grpc.ErrorMessage;
import org.junit.jupiter.api.*;

import static org.mockito.Mockito.*;

/**
 * Tests for the {@link BufferingReplyChannel}.
 */
class BufferingReplyChannelTest {

    private BufferingReplyChannel<String> bufferingReplyChannel;
    private ReplyChannel<String> delegate;
    private CloseableBuffer<String> buffer;

    @BeforeEach
    void setUp() {
        delegate = mock(ReplyChannel.class);
        buffer = mock(CloseableBuffer.class);
        bufferingReplyChannel = new BufferingReplyChannel<>(delegate, buffer);
    }

    @Test
    void bufferSend() {
        bufferingReplyChannel.send("message");
        verify(buffer).put("message");
        verifyNoInteractions(delegate);
    }

    @Test
    void bufferComplete() {
        bufferingReplyChannel.complete();
        verify(buffer).close();
        verifyNoInteractions(delegate);
    }

    @Test
    void bufferCompleteWithError() {
        ErrorMessage errorMessage = ErrorMessage.getDefaultInstance();
        bufferingReplyChannel.completeWithError(errorMessage);
        verify(buffer).closeExceptionally(errorMessage);

        bufferingReplyChannel.completeWithError(ErrorCategory.OTHER, "msg");
        verify(buffer).closeExceptionally(ErrorMessage.newBuilder()
                                                      .setErrorCode(ErrorCategory.OTHER.errorCode())
                                                      .setMessage("msg")
                                                      .build());

        verifyNoInteractions(delegate);
    }

    @Test
    void delegateAck() {
        bufferingReplyChannel.sendAck();
        verify(delegate).sendAck();
        verifyNoInteractions(buffer);
    }

    @Test
    void delegateNack() {
        ErrorMessage errorMessage = ErrorMessage.getDefaultInstance();
        bufferingReplyChannel.sendNack(errorMessage);
        verify(delegate).sendNack(errorMessage);
        verifyNoInteractions(buffer);
    }
}
