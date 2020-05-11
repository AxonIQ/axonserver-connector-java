package io.axoniq.axonserver.connector.testutils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AxonServerUtils {

    public static void purgeEventsFromAxonServer(String hostname, int port) throws IOException {
        final URL url = new URL(String.format("http://%s:%d%s", hostname, port, "/v1/devmode/purge-events"));
        HttpURLConnection connection =  null;
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("DELETE");
            connection.getInputStream().close();
            assertEquals(200, connection.getResponseCode());
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
