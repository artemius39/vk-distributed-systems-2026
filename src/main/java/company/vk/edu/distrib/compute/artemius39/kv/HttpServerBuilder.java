package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class HttpServerBuilder {
    private final Map<String, Map<String, HttpHandler>> handlers;
    private String host;
    private int port;
    private int backlog;

    public HttpServerBuilder() {
        handlers = new HashMap<>();
    }

    public HttpServerBuilder host(String host) {
        this.host = host;
        return this;
    }

    public HttpServerBuilder port(int port) {
        this.port = port;
        return this;
    }

    public HttpServerBuilder backlog(int backlog) {
        this.backlog = backlog;
        return this;
    }

    public HttpServerBuilder endpoint(String method, String route, HttpHandler handler) {
        handlers.computeIfAbsent(route, k -> new HashMap<>()).put(method, handler);
        return this;
    }

    public HttpServer build() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(host, port), backlog);
        handlers.forEach((route, handlersByMethod) -> server.createContext(route, exchange -> {
            try (exchange) {
                HttpHandler handler = handlersByMethod.get(exchange.getRequestMethod());
                if (handler == null) {
                    HttpUtils.sendEmpty(exchange, HttpCodes.METHOD_NOT_ALLOWED);
                } else {
                    handler.handle(exchange);
                }
            }
        }));
        return server;
    }
}
