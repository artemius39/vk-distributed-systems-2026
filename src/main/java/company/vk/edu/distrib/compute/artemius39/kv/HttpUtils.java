package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;

public final class HttpUtils {
    public static final int EMPTY_RESPONSE_LENGTH = -1;
    public static final String ENTITY_PATH = "/v0/entity";

    private HttpUtils() {
        // utility class
    }

    public static void sendEmpty(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, EMPTY_RESPONSE_LENGTH);
    }

    // Каждый вызов метода создаёт свою мапу и работает с ней, нет смысла в ConcurrentHashMap
    @SuppressWarnings("PMD.UseConcurrentHashMap")
    public static Map<String, List<String>> parseQueryParams(HttpExchange exchange) {
        Map<String, List<String>> params = new LinkedHashMap<>();
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null || query.isEmpty()) {
            return params;
        }

        for (String parameter : query.split("&")) {
            if (parameter.isEmpty()) {
                continue;
            }
            String[] pair = parameter.split("=", 2);
            String name = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);
            String value = pair.length == 2
                ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8)
                : "";
            params.computeIfAbsent(name, ignored -> new ArrayList<>()).add(value);
        }
        return params;
    }
}
