/*
 * Copyright (c) 2020-2024. AxonIQ
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
package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.impl.AssertUtils;

import java.text.ParseException;
import java.util.List;

/**
 * Defines the properties for a new persistent stream.
 *
 * @author Marc Gathier
 * @since 2024.0.0
 */
public class PersistentStreamProperties {
    public static final String HEAD_POSITION = "HEAD";
    public static final String TAIL_POSITION = "TAIL";
    private final String streamName;
    private final int segments;
    private final String sequencingPolicyName;
    private final List<String> sequencingPolicyParameters;
    private final String initialPosition;
    private final String filter;

    /**
     * Instantiates the persistent stream properties
     *
     * @param streamName                 a logical name for the persistent stream
     * @param segments                   the number of segments, must be larger than 0
     * @param sequencingPolicyName       the sequencing policy name, must be a name known in Axon Server
     * @param sequencingPolicyParameters optional parameters for the sequencing policy
     * @param initialPosition            first token to read or HEAD or TAIL
     * @param filter                     an optional filter to filter events on Axon Server side
     */
    public PersistentStreamProperties(String streamName, int segments, String sequencingPolicyName,
                                      List<String> sequencingPolicyParameters, String initialPosition, String filter) {
        AssertUtils.assertParameter(segments > 0, "Segments must be > 0");
        AssertUtils.assertParameter(valid(initialPosition), "initialPosition must be >= 0 or 'HEAD' or 'TAIL'");
        AssertUtils.assertParameter(sequencingPolicyName != null,
                                    "sequencingPolicyName must not be null");

        this.streamName = streamName;
        this.segments = segments;
        this.sequencingPolicyName = sequencingPolicyName;
        this.sequencingPolicyParameters = sequencingPolicyParameters;
        this.initialPosition = initialPosition;
        this.filter = filter;
    }

    private boolean valid(String initialPosition) {
        if (HEAD_POSITION.equals(initialPosition) || TAIL_POSITION.equals(initialPosition)) {
            return true;
        }

        try {
            long initial = Long.parseLong(initialPosition);
            return initial >= 0;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    /**
     * Returns the logical name for the persistent stream.
     *
     * @return the logical name for the persistent stream
     */
    public String streamName() {
        return streamName;
    }

    /**
     * Returns the number of segments.
     * @return the number of segments
     */
    public int segments() {
        return segments;
    }

    /**
     * Returns the sequencing policy name.
     * @return the sequencing policy name
     */
    public String sequencingPolicyName() {
        return sequencingPolicyName;
    }

    /**
     * Returns the parameters for the sequencing policy.
     * @return parameters for the sequencing policy
     */
    public List<String> sequencingPolicyParameters() {
        return sequencingPolicyParameters;
    }

    /**
     * Returns the first token to read.
     * @return first token to read
     */
    public String initialPosition() {
        return initialPosition;
    }

    /**
     * Returns the filter to filter events on Axon Server side.
     * @return filter to filter events on Axon Server side
     */
    public String filter() {
        return filter;
    }
}
