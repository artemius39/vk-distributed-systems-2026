package company.vk.edu.distrib.compute.artemius39.urlshortener;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.SecureRandom;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

public class UrlShortenerServiceImpl implements UrlShortenerService {
    private static final String LINKS_URI = "/v0/links/";
    private static final String ALPHABET = IntStream.concat(
            IntStream.concat(
                IntStream.rangeClosed('0', '9'),
                IntStream.rangeClosed('a', 'z')
            ),
            IntStream.rangeClosed('A', 'Z')
        ).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
    private static final int SHORT_URL_LENGTH = 10;
    private static final String ROOT_URI = "/";
    private static final int SERVER_STOP_DELAY = 1;
    private static final String HOSTNAME = "localhost";

    private final HttpServer server;
    private final Dao<String> linkDao;
    private final Dao<String> authDao;
    private final SecureRandom random;

    public UrlShortenerServiceImpl(int port, Dao<String> linkDao, Dao<String> authDao) throws IOException {
        this.linkDao = linkDao;
        this.server = createServer(port);
        this.authDao = authDao;
        this.random = new SecureRandom();
    }

    private HttpServer createServer(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(HOSTNAME, port), 0);
        server.createContext(
            "/v0/status", exchange -> {
                try (exchange) {
                    if ("GET".equals(exchange.getRequestMethod())) {
                        getStatus(exchange);
                    } else {
                        HttpUtils.methodNotAllowed(exchange);
                    }
                }
            }
        );
        server.createContext(
            LINKS_URI, exchange -> {
                try (exchange) {
                    switch (exchange.getRequestMethod()) {
                        case "GET" -> getLink(exchange);
                        case "PUT" -> changeLink(exchange);
                        case "DELETE" -> deleteLink(exchange);
                        default -> HttpUtils.methodNotAllowed(exchange);
                    }
                }
            }
        );
        server.createContext(
            "/v0/links", exchange -> {
                try (exchange) {
                    if ("POST".equals(exchange.getRequestMethod())) {
                        createLink(exchange);
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
                        redirectLink(exchange);
                    } else {
                        HttpUtils.methodNotAllowed(exchange);
                    }
                }
            }
        );
        server.createContext(
            "/internal/users", exchange -> {
                try (exchange) {
                    if ("POST".equals(exchange.getRequestMethod())) {
                        registerUser(exchange);
                    } else {
                        HttpUtils.methodNotAllowed(exchange);
                    }
                }
            }
        );

        return server;
    }

    private void registerUser(HttpExchange exchange) throws IOException {
        String requestString = HttpUtils.getRequestBodyAsString(exchange);
        String[] split = requestString.split(":");
        if (split.length != 2) {
            HttpUtils.unprocessableEntity(exchange);
            return;
        }
        String username = split[0];
        String password = split[1];
        authDao.upsert(username, password);
        exchange.sendResponseHeaders(200, -1);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        try {
            server.stop(SERVER_STOP_DELAY);
            linkDao.close();
            authDao.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void getStatus(HttpExchange httpExchange) throws IOException {
        httpExchange.sendResponseHeaders(200, -1);
    }

    private void getLink(HttpExchange httpExchange) throws IOException {
        if (!checkAuth(httpExchange)) {
            return;
        }
        String id = HttpUtils.getPathParam(httpExchange, LINKS_URI);
        if (!isValidShortLink(httpExchange, id)) {
            return;
        }
        try {
            String longLink = linkDao.get(id);
            HttpUtils.sendResponse(httpExchange, 200, longLink);
        } catch (NoSuchElementException e) {
            HttpUtils.notFound(httpExchange);
        } catch (IllegalArgumentException e) {
            HttpUtils.unprocessableEntity(httpExchange);
        }
    }

    private void createLink(HttpExchange exchange) throws IOException {
        if (!checkAuth(exchange)) {
            return;
        }
        String longLink = HttpUtils.getRequestBodyAsString(exchange);
        if (!isValidLongLink(exchange, longLink)) {
            return;
        }
        String shortLink = generateShortUrl();
        linkDao.upsert(shortLink, longLink);
        String fullLink = getFullLink(shortLink);
        HttpUtils.sendResponse(exchange, 201, fullLink);
    }

    private void changeLink(HttpExchange exchange) throws IOException {
        if (!checkAuth(exchange)) {
            return;
        }
        String linkToChange = HttpUtils.getPathParam(exchange, LINKS_URI);
        if (!isValidShortLink(exchange, linkToChange)) {
            return;
        }
        String newLink = HttpUtils.getRequestBodyAsString(exchange);
        if (!isValidLongLink(exchange, newLink)) {
            return;
        }
        try {
            linkDao.get(linkToChange);
            linkDao.upsert(linkToChange, newLink);
            exchange.sendResponseHeaders(200, -1);
        } catch (NoSuchElementException e) {
            HttpUtils.notFound(exchange);
        } catch (IllegalArgumentException e) {
            HttpUtils.unprocessableEntity(exchange);
        }
    }

    private void deleteLink(HttpExchange exchange) throws IOException {
        if (!checkAuth(exchange)) {
            return;
        }
        String linkToDelete = HttpUtils.getPathParam(exchange, LINKS_URI);
        if (!isValidShortLink(exchange, linkToDelete)) {
            return;
        }
        linkDao.delete(linkToDelete);
        exchange.sendResponseHeaders(202, -1);
    }

    private void redirectLink(HttpExchange exchange) throws IOException {
        String shortLink = HttpUtils.getPathParam(exchange, ROOT_URI);
        if (!isValidShortLink(exchange, shortLink)) {
            return;
        }
        try {
            String longLink = linkDao.get(shortLink);
            exchange.getResponseHeaders().set("Location", longLink);
            exchange.sendResponseHeaders(301, -1);
        } catch (NoSuchElementException e) {
            HttpUtils.notFound(exchange);
        } catch (IllegalArgumentException e) {
            HttpUtils.unprocessableEntity(exchange);
        }
    }

    private String getFullLink(String shortLink) {
        return "http://%s:%d/%s".formatted(HOSTNAME, server.getAddress().getPort(), shortLink);
    }

    private boolean checkAuth(HttpExchange httpExchange) throws IOException {
        Credentials credentials = HttpUtils.parseAuth(httpExchange);
        if (credentials != null) {
            try {
                String expectedPassword = authDao.get(credentials.username());
                if (credentials.password().equals(expectedPassword)) {
                    return true;
                }
            } catch (NoSuchElementException | IllegalArgumentException | IOException e) {
                HttpUtils.unauthorized(httpExchange);
                return false;
            }
        }
        HttpUtils.unauthorized(httpExchange);
        return false;
    }

    private String generateShortUrl() throws IOException {
        while (true) {
            StringBuilder sb = new StringBuilder(SHORT_URL_LENGTH);
            for (int i = 0; i < SHORT_URL_LENGTH; i++) {
                sb.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
            }
            String result = sb.toString();
            try {
                linkDao.get(result);
            } catch (NoSuchElementException e) {
                return result;
            }
        }
    }

    private boolean isValidShortLink(HttpExchange exchange, String shortLink) throws IOException {
        if (shortLink.codePoints().anyMatch(ch -> ALPHABET.indexOf(ch) == -1)) {
            exchange.sendResponseHeaders(422, -1);
            return false;
        }
        return true;
    }

    private boolean isValidLongLink(HttpExchange exchange, String longLink) throws IOException {
        try {
            URI uri = URI.create(longLink);
            if (("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                && uri.getHost() != null) {
                return true;
            }
        } catch (IllegalArgumentException e) {
            HttpUtils.unprocessableEntity(exchange);
            return false;
        }
        HttpUtils.unprocessableEntity(exchange);
        return false;
    }
}
