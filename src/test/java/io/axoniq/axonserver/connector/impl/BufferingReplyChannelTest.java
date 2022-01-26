/*
 * Copyright (c) 2020-2022. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.grpc.ErrorMessage;
import org.junit.jupiter.api.*;

import static org.mockito.Mockito.*;

/**
 * Tests for {@link BufferingReplyChannel}.
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
        verifyZeroInteractions(delegate);
    }

    @Test
    void bufferComplete() {
        bufferingReplyChannel.complete();
        verify(buffer).close();
        verifyZeroInteractions(delegate);
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

        verifyZeroInteractions(delegate);
    }

    @Test
    void delegateAck() {
        bufferingReplyChannel.sendAck();
        verify(delegate).sendAck();
        verifyZeroInteractions(buffer);
    }

    @Test
    void delegateNack() {
        ErrorMessage errorMessage = ErrorMessage.getDefaultInstance();
        bufferingReplyChannel.sendNack(errorMessage);
        verify(delegate).sendNack(errorMessage);
        verifyZeroInteractions(buffer);
    }
}
