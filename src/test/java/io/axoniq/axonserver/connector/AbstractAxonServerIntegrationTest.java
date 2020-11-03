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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.okhttp3.Call;
import org.testcontainers.shaded.okhttp3.OkHttpClient;
import org.testcontainers.shaded.okhttp3.Request;
import org.testcontainers.shaded.okhttp3.RequestBody;
import org.testcontainers.shaded.okhttp3.Response;
import org.testcontainers.shaded.okhttp3.internal.Util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.function.BiFunction;

@Testcontainers
public abstract class AbstractAxonServerIntegrationTest {

    private static final Gson GSON = new Gson();
    public static Network network = Network.newNetwork();

    @Container
    public static GenericContainer<?> axonServerContainer =
            new GenericContainer<>(System.getProperty("AXON_SERVER_IMAGE", "axoniq/axonserver"))
                    .withExposedPorts(8024, 8124)
                    .withEnv("AXONIQ_AXONSERVER_NAME", "axonserver")
                    .withEnv("AXONIQ_AXONSERVER_HOSTNAME", "localhost")
                    .withEnv("AXONIQ_AXONSERVER_DEVMODE_ENABLED", "true")
                    .withImagePullPolicy(PullPolicy.ageBased(Duration.ofDays(1)))
                    .withNetwork(network)
                    .withNetworkAliases("axonserver")
                    .waitingFor(Wait.forHttp("/actuator/health").forPort(8024));

    @Container
    public static GenericContainer<?> toxiProxyContainer =
            new GenericContainer<>("shopify/toxiproxy")
                    .withExposedPorts(8474, 8124)
                    .withImagePullPolicy(PullPolicy.ageBased(Duration.ofDays(1)))
                    .withNetwork(network)
                    .waitingFor(Wait.forHttp("/version").forPort(8474));

    protected static Proxy axonServerProxy;

    protected static ServerAddress axonServerAddress;
    private static ServerAddress axonServerHttpPort;
    protected String axonServerVersion;

    @BeforeAll
    static void initialize() throws IOException {
        axonServerAddress = new ServerAddress(toxiProxyContainer.getContainerIpAddress(), toxiProxyContainer.getMappedPort(8124));
        axonServerHttpPort = new ServerAddress(axonServerContainer.getContainerIpAddress(), axonServerContainer.getMappedPort(8024));
        ToxiproxyClient client = new ToxiproxyClient(toxiProxyContainer.getContainerIpAddress(), toxiProxyContainer.getMappedPort(8474));
        axonServerProxy = getOrCreateProxy(client, "axonserver", "0.0.0.0:8124", "axonserver:8124");

    }

    @BeforeEach
    void prepareInstance() throws IOException {
        JsonElement info = sendToAxonServer((r, b) -> r.get(), "/actuator/info");
        axonServerVersion = info.getAsJsonObject().getAsJsonObject("app").get("version").getAsString();

        for (Toxic toxic : axonServerProxy.toxics().getAll()) {
            toxic.remove();
        }
        axonServerProxy.enable();
        AxonServerUtils.purgeEventsFromAxonServer(axonServerHttpPort.getHostName(), axonServerHttpPort.getGrpcPort());
    }

    public static Proxy getOrCreateProxy(ToxiproxyClient client, String proxyName, String listen, String upstream) throws IOException {
        Proxy proxy = client.getProxyOrNull(proxyName);
        if (proxy == null) {
            proxy = client.createProxy(proxyName, listen, upstream);
        }
        return proxy;
    }

    protected JsonElement sendToAxonServer(BiFunction<Request.Builder, RequestBody, Request.Builder> method, String path) throws IOException {
        OkHttpClient client = new OkHttpClient();
        Call call = client.newCall(method.apply(new Request.Builder()
                                                        .url("http://" + axonServerContainer.getContainerIpAddress() + ":" + axonServerContainer.getMappedPort(8024) + path),
                                                Util.EMPTY_REQUEST)
                                         .build());
        Response result = call.execute();
        JsonElement read = new JsonObject();
        if (result.code() != 200) {
            throw new IOException("Got error code " + result.code() + (result.body() == null ? "" : " - " + result.body().string()));
        } else if (result.body() == null) {
            return read;
        } else {
            read = GSON.fromJson(new InputStreamReader(result.body().byteStream()), JsonElement.class);
        }
        return read;
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
        final URL url = new URL(String.format("http://%s:%d%s", axonServerContainer.getContainerIpAddress(), axonServerContainer.getMappedPort(8024), path));
        return (HttpURLConnection) url.openConnection();
    }
}
