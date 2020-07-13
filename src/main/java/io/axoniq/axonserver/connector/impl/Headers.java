/*
 * Copyright (c) 2020. AxonIQ
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

import io.grpc.Metadata;

public class Headers {

    public static final Metadata.Key<String> CONTEXT =
            Metadata.Key.of("AxonIQ-Context", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> ACCESS_TOKEN =
            Metadata.Key.of("AxonIQ-Access-Token", Metadata.ASCII_STRING_MARSHALLER);


    private Headers() {
    }
}
