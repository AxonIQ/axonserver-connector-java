package io.axoniq.axonserver.connector;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.connector.testutils.AxonServerUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

@Testcontainers
public abstract class AbstractAxonServerIntegrationTest {

    public static Network network = Network.newNetwork();

    @Container
    public static GenericContainer<?> axonServerContainer =
            new GenericContainer<>("axoniq/axonserver")
                    .withExposedPorts(8024, 8124)
                    .withEnv("AXONIQ_AXONSERVER_NAME", "axonserver")
                    .withEnv("AXONIQ_AXONSERVER_HOSTNAME", "localhost")
                    .withEnv("AXONIQ_AXONSERVER_DEVMODE_ENABLED", "true")
                    .withNetwork(network)
                    .withNetworkAliases("axonserver")
                    .waitingFor(Wait.forHttp("/actuator/health").forPort(8024));

    @Container
    public static GenericContainer<?> toxiProxyContainer =
            new GenericContainer<>("shopify/toxiproxy")
                    .withExposedPorts(8474, 8124)
                    .withNetwork(network)
                    .waitingFor(Wait.forHttp("/version").forPort(8474));


    protected static Proxy axonServerProxy;

    protected static ServerAddress axonServerAddress;
    private static ServerAddress axonServerHttpPort;

    @BeforeAll
    static void initialize() throws IOException {
        axonServerAddress = new ServerAddress(toxiProxyContainer.getContainerIpAddress(), toxiProxyContainer.getMappedPort(8124));
        axonServerHttpPort = new ServerAddress(axonServerContainer.getContainerIpAddress(), axonServerContainer.getMappedPort(8024));
        ToxiproxyClient client = new ToxiproxyClient(toxiProxyContainer.getContainerIpAddress(), toxiProxyContainer.getMappedPort(8474));
        axonServerProxy = getOrCreateProxy(client, "axonserver", "0.0.0.0:8124", "axonserver:8124");
    }

    @BeforeEach
    void prepareInstance() throws IOException {
        AxonServerUtils.purgeEventsFromAxonServer(axonServerHttpPort.hostName(), axonServerHttpPort.grpcPort());
    }

    public static Proxy getOrCreateProxy(ToxiproxyClient client, String proxyName, String listen, String upstream) throws IOException {
        Proxy proxy = client.getProxyOrNull(proxyName);
        if (proxy == null) {
            proxy = client.createProxy(proxyName, listen, upstream);
        }
        return proxy;
    }

}
