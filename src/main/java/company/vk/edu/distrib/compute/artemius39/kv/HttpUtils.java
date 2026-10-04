package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;

public final class HttpUtils {
    public static final int EMPTY_RESPONSE_LENGTH = -1;
    public static final String ENTITY_PREFIX = "/v0/entity/";

    private HttpUtils() {
        // utility class
    }

    public static void sendEmpty(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, EMPTY_RESPONSE_LENGTH);
    }
}
