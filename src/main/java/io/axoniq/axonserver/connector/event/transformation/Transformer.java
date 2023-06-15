/*
 * Copyright (c) 2020-2023. AxonIQ
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

package io.axoniq.axonserver.connector.event.transformation;

/**
 * Functional interface that needs to be implemented to register transformation actions into the Transformation.
 *
 * @author Sara Pellegrini
 * @since 2023.1.0
 */
@FunctionalInterface
public interface Transformer {

    /**
     * Registers the requested event changes to the event store.
     *
     * @param appender the appender used to register the transformation actions into the Transformation, since it
     *                 provides the methods to delete and replace events
     */
    void transform(Appender appender);
}
