package io.axoniq.axonserver.connector.admin;

/**
 * @author Stefan Dragisic
 */
public enum LoadBalanceStrategy {

    DEFAULT("default"),
    THREAD_NUMBER("threadNumber");

    private final String name;

    LoadBalanceStrategy(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public io.axoniq.axonserver.grpc.admin.LoadBalanceStrategy toGrpc() {
        switch (this) {
            case DEFAULT : return io.axoniq.axonserver.grpc.admin.LoadBalanceStrategy.DEFAULT;
            case THREAD_NUMBER : return io.axoniq.axonserver.grpc.admin.LoadBalanceStrategy.THREAD_NUMBER;
            default : throw new IllegalArgumentException("Unknown strategy " + this);
        }
    }

    public static LoadBalanceStrategy fromString(String name) {
        for (LoadBalanceStrategy strategy : LoadBalanceStrategy.values()) {
            if (strategy.getName().equalsIgnoreCase(name)) {
                return strategy;
            }
        }
        throw new IllegalArgumentException("No strategy with name " + name + " found");
    }
}
