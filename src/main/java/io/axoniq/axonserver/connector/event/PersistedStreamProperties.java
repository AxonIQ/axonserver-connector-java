package io.axoniq.axonserver.connector.event;

import java.util.List;

public class PersistedStreamProperties {
    private final String streamName;
    private final int segments;
    private final String sequencingPolicyName;
    private final List<String> sequencingPolicyParameters;
    private final long initialPosition;
    private final String filter;

    public PersistedStreamProperties(String streamName, int segments, String sequencingPolicyName,
                                     List<String> sequencingPolicyParameters, long initialPosition, String filter) {
        this.streamName = streamName;
        this.segments = segments;
        this.sequencingPolicyName = sequencingPolicyName;
        this.sequencingPolicyParameters = sequencingPolicyParameters;
        this.initialPosition = initialPosition;
        this.filter = filter;
    }

    public String streamName() {
        return streamName;
    }

    public int segments() {
        return segments;
    }

    public String sequencingPolicyName() {
        return sequencingPolicyName;
    }

    public List<String> sequencingPolicyParameters() {
        return sequencingPolicyParameters;
    }

    public long initialPosition() {
        return initialPosition;
    }

    public String filter() {
        return filter;
    }
}
