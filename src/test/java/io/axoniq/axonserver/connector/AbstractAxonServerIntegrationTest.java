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

package io.axoniq.axonserver.connector;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.connector.testutils.AxonServerUtils;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.function.Function;

@Testcontainers
public abstract class AbstractAxonServerIntegrationTest {

    private static final Gson GSON = new Gson();
    public static Network network = Network.newNetwork();

    @SuppressWarnings("resource")
    @Container
    public static GenericContainer<?> axonServerContainer =
            new GenericContainer<>(getDockerImageName())
                    .withExposedPorts(8024, 8124)
                    .withEnv("AXONIQ_AXONSERVER_PREVIEW_EVENT_TRANSFORMATION", "true")
                    .withEnv("AXONIQ_AXONSERVER_NAME", "axonserver")
                    .withEnv("AXONIQ_AXONSERVER_HOSTNAME", "localhost")
                    .withEnv("AXONIQ_AXONSERVER_DEVMODE_ENABLED", "true")
                    .withEnv("AXONIQ_AXONSERVER_ACCESSCONTROL_TOKEN", "user-token")
                    .withEnv("AXONIQ_AXONSERVER_ACCESSCONTROL_ADMIN_TOKEN", "admin-token")
                    .withImagePullPolicy(PullPolicy.ageBased(Duration.ofDays(1)))
                    .withNetwork(network)
                    .withNetworkAliases("axonserver")
                    .waitingFor(Wait.forHttp("/actuator/health").forPort(8024));

    private static String getDockerImageName() {
        String envVariable = System.getenv("AXON_SERVER_IMAGE");
        return envVariable != null ? envVariable : System.getProperty("AXON_SERVER_IMAGE", "axoniq/axonserver");
    }

    @SuppressWarnings("resource")
    @Container
    public static GenericContainer<?> toxiProxyContainer =
            new GenericContainer<>("shopify/toxiproxy")
                    .withExposedPorts(8474, 8124)
                    .withImagePullPolicy(PullPolicy.ageBased(Duration.ofDays(1)))
                    .withNetwork(network)
                    .waitingFor(Wait.forHttp("/version").forPort(8474));

    protected static Proxy axonServerProxy;

    protected static ServerAddress axonServerAddress;
    protected static ServerAddress axonServerHttpPort;
    protected String axonServerVersion;

    @BeforeAll
    static void initialize() throws IOException {
        axonServerAddress = new ServerAddress("localhost", toxiProxyContainer.getMappedPort(8124));
        axonServerHttpPort = new ServerAddress(axonServerContainer.getHost(), axonServerContainer.getMappedPort(8024));
        ToxiproxyClient client = new ToxiproxyClient(
                toxiProxyContainer.getHost(), toxiProxyContainer.getMappedPort(8474)
        );
        axonServerProxy = getOrCreateProxy(client, "axonserver", "0.0.0.0:8124", "axonserver:8124");
        AxonServerUtils.initCluster(axonServerHttpPort.getHostName(), axonServerHttpPort.getGrpcPort());
    }

    @BeforeEach
    void prepareInstance() throws IOException {
        JsonElement info = sendToAxonServer(HttpGet::new, "/actuator/info");
        axonServerVersion = info.getAsJsonObject().getAsJsonObject("app").get("version").getAsString();

        for (Toxic toxic : axonServerProxy.toxics().getAll()) {
            toxic.remove();
        }
        axonServerProxy.enable();
        AxonServerUtils.purgeEventsFromAxonServer(axonServerHttpPort.getHostName(),
                                                  axonServerHttpPort.getGrpcPort(),
                                                  dcbContext());
    }

    public static Proxy getOrCreateProxy(ToxiproxyClient client,
                                         String proxyName,
                                         String listen,
                                         String upstream) throws IOException {
        Proxy proxy = client.getProxyOrNull(proxyName);
        if (proxy == null) {
            proxy = client.createProxy(proxyName, listen, upstream);
        }
        return proxy;
    }

    protected boolean dcbContext() {
        return false;
    }

    protected JsonElement sendToAxonServer(Function<String, ClassicHttpRequest> method,
                                           String path) throws IOException {
        //noinspection HttpUrlsUsage
        String uri = "http://" + axonServerContainer.getHost() + ":" + axonServerContainer.getMappedPort(8024) + path;
        ClassicHttpRequest request = method.apply(uri);

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            try (CloseableHttpResponse response = httpclient.execute(request)) {

                int code = response.getCode();
                String content = EntityUtils.toString(response.getEntity());
                if (code != 200) {
                    throw new IOException("Got error code " + code + (content == null ? "" : " - " + content));
                } else if (content == null) {
                    return new JsonObject();
                } else {
                    return GSON.fromJson(content, JsonElement.class);
                }
            }
        } catch (ParseException e) {
            throw new IOException(e);
        }
    }

    protected JsonElement getFromAxonServer(String path) throws IOException {
        HttpURLConnection connection = getConnection(path);
        return readResponse(connection);
    }

    private JsonElement readResponse(HttpURLConnection connection) throws IOException {
        final int status = connection.getResponseCode();
        if (status != 200) {
            throw new IOException("Got error code" + status);
        }
        return GSON.fromJson(new InputStreamReader(connection.getInputStream()), JsonElement.class);
    }

    private HttpURLConnection getConnection(String path) throws IOException {
        //noinspection HttpUrlsUsage
        final URL url = new URL(String.format("http://%s:%d%s",
                                              axonServerContainer.getHost(),
                                              axonServerContainer.getMappedPort(8024),
                                              path));
        return (HttpURLConnection) url.openConnection();
    }
}
