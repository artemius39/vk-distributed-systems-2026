package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.util.NoSuchElementException;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

public class KVServiceHandler implements AutoCloseable {
    private final Dao<byte[]> dao;

    public KVServiceHandler(Dao<byte[]> dao) {
        this.dao = dao;
    }

    public void status(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(HttpCodes.OK, HttpUtils.EMPTY_RESPONSE_LENGTH);
    }

    public void getByKey(HttpExchange exchange) throws IOException {
        String key = parseKey(exchange);
        if (key == null) {
            HttpUtils.sendEmpty(exchange, HttpCodes.BAD_REQUEST);
            return;
        }
        byte[] value;
        try {
            value = dao.get(key);
        } catch (NoSuchElementException e) {
            HttpUtils.sendEmpty(exchange, HttpCodes.NOT_FOUND);
            return;
        } catch (IllegalArgumentException e) {
            HttpUtils.sendEmpty(exchange, HttpCodes.BAD_REQUEST);
            return;
        } catch (IOException e) {
            HttpUtils.sendEmpty(exchange, HttpCodes.INTERNAL_SERVER_ERROR);
            return;
        }
        exchange.sendResponseHeaders(HttpCodes.OK, value.length);
        exchange.getResponseBody().write(value);
    }

    public void upsertByKey(HttpExchange exchange) throws IOException {
        String key = parseKey(exchange);
        if (key == null) {
            HttpUtils.sendEmpty(exchange, HttpCodes.BAD_REQUEST);
            return;
        }

        byte[] value;
        try {
            value = exchange.getRequestBody().readAllBytes();
        } catch (IOException e) {
            HttpUtils.sendEmpty(exchange, HttpCodes.INTERNAL_SERVER_ERROR);
            return;
        }

        try {
            dao.upsert(key, value);
        } catch (IllegalArgumentException e) {
            HttpUtils.sendEmpty(exchange, HttpCodes.BAD_REQUEST);
            return;
        } catch (IOException e) {
            HttpUtils.sendEmpty(exchange, HttpCodes.INTERNAL_SERVER_ERROR);
            return;
        }

        HttpUtils.sendEmpty(exchange, HttpCodes.CREATED);
    }

    public void deleteByKey(HttpExchange exchange) throws IOException {
        String key = parseKey(exchange);
        if (key == null) {
            HttpUtils.sendEmpty(exchange, HttpCodes.BAD_REQUEST);
            return;
        }

        try {
            dao.delete(key);
            HttpUtils.sendEmpty(exchange, HttpCodes.ACCEPTED);
        } catch (IllegalArgumentException e) {
            HttpUtils.sendEmpty(exchange, HttpCodes.BAD_REQUEST);
        } catch (IOException e) {
            HttpUtils.sendEmpty(exchange, HttpCodes.INTERNAL_SERVER_ERROR);
        }
    }

    private String parseKey(HttpExchange exchange) {
        String path = exchange.getRequestURI().getPath();
        if (!path.startsWith(HttpUtils.ENTITY_PREFIX)) {
            return null;
        }
        String key = path.substring(HttpUtils.ENTITY_PREFIX.length());
        return key.isEmpty() ? null : key;
    }

    @Override
    public void close() throws IOException {
        dao.close();
    }
}
