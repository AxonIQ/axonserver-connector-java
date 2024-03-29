/*
 * Copyright (c) 2020-2022. AxonIQ
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

package io.axoniq.axonserver.connector;

import io.axoniq.axonserver.grpc.ErrorMessage;

/**
 * Interface providing access to the communication channel to send replies to incoming instructions. All instructions
 * must be acknowledged by completing the reply channel after successfully sending all necessary replies.
 * <p>
 * One should not send messages after completing the reply channel. The behavior is such a case is undefined.
 *
 * @param <T> the type of message expected on this {@link ReplyChannel}
 */
public interface ReplyChannel<T> {

    /**
     * Sends the given {@code outboundMessage} as a reply to an incoming instruction. Where possible, the instruction
     * identifiers should be set on the outbound message.
     *
     * @param outboundMessage the message to send
     * @implNote Implementations are encouraged to validate and correct these instruction identifiers.
     */
    void send(T outboundMessage);

    /**
     * Sends the given {@code response} and marks the processing as completed, possibly signalling flow control that
     * more query messages may be sent.
     * <p>
     * No more responses should be sent after invoking this method. The behavior in that case is undefined, and these
     * messages are likely to be ignored.
     */
    default void sendLast(T outboundMessage) {
        try {
            send(outboundMessage);
        } finally {
            complete();
        }
    }

    /**
     * Sends a receipt acknowledgement if one hasn't been sent yet for this instruction.
     * If the incoming instruction has no instruction ID, this method does nothing.
     *
     * @see #complete()
     * @deprecated This method is deprecated because it is not needed anymore, as the {@link #complete()} already sends
     * positive result, indicating that the incoming message has been handled as expected.
     */
    @Deprecated
    default void sendAck() {
    }

    /**
     * Sends a negative acknowledgement, indicating that the incoming message could not be handled as expected. Unlike
     * {@link #sendNack(ErrorMessage)}, no specific error details are provided.
     * If the incoming instruction has no instruction ID, this method does nothing.
     *
     * @see #completeWithError(ErrorMessage)
     * @see #completeWithError(ErrorCategory, String)
     * @deprecated This method is deprecated because it is not needed anymore, as {@link #completeWithError(ErrorMessage)}
     * or {@link #completeWithError(ErrorCategory, String)} sends a failed result, indicating that the incoming message
     * could not be handled as expected.
     */
    @Deprecated
    default void sendNack() {
        sendNack(ErrorMessage.getDefaultInstance());
    }

    /**
     * Sends a negative acknowledgement, indicating that the incoming message could not be handled as expected, using
     * given {@code errorMessage} to describe the reason. The given {@code errorMessage} should provide sufficient
     * information about the error. If the incoming instruction has no instruction ID, this method does nothing.
     *
     * @see #completeWithError(ErrorMessage)
     * @see #completeWithError(ErrorCategory, String)
     * @deprecated This method is deprecated because it is not needed anymore, as {@link #completeWithError(ErrorMessage)}
     * or {@link #completeWithError(ErrorCategory, String)} already sends a failed result, indicating that the incoming
     * message could not be handled as expected.
     */
    @Deprecated
    default void sendNack(ErrorMessage errorMessage) {
    }

    /**
     * Marks the inbound instruction as completed.
     */
    void complete();

    /**
     * Marks the inbound instruction as exceptionally completed. The given {@code errorMessage} should provide
     * sufficient information about the error.
     *
     * @param errorMessage the ErrorMessage describing the error
     */
    void completeWithError(ErrorMessage errorMessage);

    /**
     * Marks the inbound instruction as exceptionally completed. The given {@code errorCategory} and {@code message}
     * should provide sufficient information about the error.
     *
     * @param errorCategory the category that best described the error encountered
     * @param message       a message providing more details about the Error
     */
    void completeWithError(ErrorCategory errorCategory, String message);
}
