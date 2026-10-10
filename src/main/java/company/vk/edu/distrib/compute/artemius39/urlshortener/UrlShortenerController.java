package company.vk.edu.distrib.compute.artemius39.urlshortener;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Objects;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

public class UrlShortenerController implements UrlShortenerService {
    public static final String LINKS_URI = "/v0/links/";
    private static final String ROOT_URI = "/";
    private static final int SERVER_STOP_DELAY = 1;
    private static final String HOSTNAME = "localhost";

    private final HttpServer server;
    private final UrlShortenerHandler handler;
    private State state = State.NEW;

    private enum State {
        NEW,
        STARTED,
        STOPPED
    }

    public UrlShortenerController(int port, UrlShortenerHandler handler) throws IOException {
        this.handler = handler;
        this.server = createServer(port);
    }

    private HttpServer createServer(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(HOSTNAME, port), 0);
        setStatusMappings(server);
        setLinksMappings(server);
        setAuthMappings(server);
        return server;
    }

    private void setStatusMappings(HttpServer server) {
        server.createContext(
            "/v0/status", exchange -> {
                try (exchange) {
                    if ("GET".equals(exchange.getRequestMethod())) {
                        handler.getStatus(exchange);
                    } else {
                        HttpUtils.methodNotAllowed(exchange);
                    }
                }
            }
        );
    }

    private void setLinksMappings(HttpServer server) {
        server.createContext(
            LINKS_URI, exchange -> {
                try (exchange) {
                    switch (exchange.getRequestMethod()) {
                        case "GET" -> handler.getLink(exchange);
                        case "PUT" -> handler.changeLink(exchange);
                        case "DELETE" -> handler.deleteLink(exchange);
                        default -> HttpUtils.methodNotAllowed(exchange);
                    }
                }
            }
        );
        server.createContext(
            "/v0/links", exchange -> {
                try (exchange) {
                    if ("POST".equals(exchange.getRequestMethod())) {
                        handler.createLink(exchange);
                    } else {
                        HttpUtils.methodNotAllowed(exchange);
                    }
                }
            }
        );
        server.createContext(
            ROOT_URI, exchange -> {
                try (exchange) {
                    if ("GET".equals(exchange.getRequestMethod())) {
                        handler.redirectLink(exchange);
                    } else {
                        HttpUtils.methodNotAllowed(exchange);
                    }
                }
            }
        );
    }

    private void setAuthMappings(HttpServer server) {
        server.createContext(
            "/internal/users", exchange -> {
                try (exchange) {
                    if ("POST".equals(exchange.getRequestMethod())) {
                        handler.registerUser(exchange);
                    } else {
                        HttpUtils.methodNotAllowed(exchange);
                    }
                }
            }
        );
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        if (state != State.NEW) {
            throw new IllegalStateException("Links DAO can only be set before start or stop");
        }
        Objects.requireNonNull(dao);
        try {
            handler.setLinksDao(dao);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void start() {
        if (state != State.NEW) {
            throw new IllegalStateException("Service has already been started or stopped");
        }
        server.start();
        state = State.STARTED;
    }

    @Override
    public void stop() {
        if (state == State.STOPPED) {
            return;
        }
        state = State.STOPPED;
        try {
            server.stop(SERVER_STOP_DELAY);
            handler.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
