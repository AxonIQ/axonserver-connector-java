/*
 * Copyright (c) 2010-2019. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector;

import static java.util.Arrays.stream;


/**
 * Converts an Axon Server Error to the relevant Axon framework exception.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public enum ErrorCode {

    // Generic errors processing client request
    AUTHENTICATION_TOKEN_MISSING("AXONIQ-1000"),
    AUTHENTICATION_INVALID_TOKEN("AXONIQ-1001"),
    UNSUPPORTED_INSTRUCTION("AXONIQ-1002"),
    INSTRUCTION_ACK_ERROR("AXONIQ-1003"),
    INSTRUCTION_EXECUTION_ERROR("AXONIQ-1004"),

    //Event publishing errors
    INVALID_EVENT_SEQUENCE("AXONIQ-2000"),
    NO_EVENT_STORE_MASTER_AVAILABLE("AXONIQ-2100"
    ),
    EVENT_PAYLOAD_TOO_LARGE(
            "AXONIQ-2001"
    ),

    //Communication errors
    CONNECTION_FAILED("AXONIQ-3001"),
    GRPC_MESSAGE_TOO_LARGE("AXONIQ-3002"),

    // Command errors
    NO_HANDLER_FOR_COMMAND("AXONIQ-4000"),
    COMMAND_EXECUTION_ERROR("AXONIQ-4002"),
    COMMAND_DISPATCH_ERROR("AXONIQ-4003"),
    CONCURRENCY_EXCEPTION("AXONIQ-4004"),

    //Query errors
    NO_HANDLER_FOR_QUERY("AXONIQ-5000"),
    QUERY_EXECUTION_ERROR("AXONIQ-5001"),
    QUERY_DISPATCH_ERROR("AXONIQ-5002"),

    // Internal errors
    DATAFILE_READ_ERROR("AXONIQ-9000"),
    INDEX_READ_ERROR("AXONIQ-9001"),
    DATAFILE_WRITE_ERROR("AXONIQ-9100"),
    INDEX_WRITE_ERROR("AXONIQ-9101"),
    DIRECTORY_CREATION_FAILED("AXONIQ-9102"),
    VALIDATION_FAILED("AXONIQ-9200"),
    TRANSACTION_ROLLED_BACK("AXONIQ-9900"),

    //Default
    OTHER("AXONIQ-0001");

    private final String errorCode;

    /**
     * Initializes the ErrorCode using the given {@code code}
     *
     * @param errorCode the code of the error
     */
    ErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public static ErrorCode getFromCode(String code) {
        return stream(values()).filter(value -> value.errorCode.equals(code)).findFirst().orElse(OTHER);
    }

    public String errorCode() {
        return errorCode;
    }

}
