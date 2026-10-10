package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;

public class RemoteDao implements Dao<byte[]> {
    private final HttpClient client;
    private final URI baseUrl;

    public RemoteDao(HttpClient client, URI baseUrl) {
        this.client = client;
        this.baseUrl = baseUrl;
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        HttpRequest request = HttpRequest.newBuilder()
            .GET()
            .uri(buildURI(key))
            .build();
        HttpResponse<byte[]> response = sendRequest(request, HttpResponse.BodyHandlers.ofByteArray());
        return switch (response.statusCode()) {
            case HttpCodes.OK -> response.body();
            case HttpCodes.NOT_FOUND -> throw new NoSuchElementException("Key not found: '" + key + "'");
            default -> throw unexpectedHttpCodeException(response);
        };
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        HttpRequest request = HttpRequest.newBuilder()
            .PUT(HttpRequest.BodyPublishers.ofByteArray(value))
            .uri(buildURI(key))
            .build();
        HttpResponse<Void> response = sendRequest(request, HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() != HttpCodes.CREATED) {
            throw unexpectedHttpCodeException(response);
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        HttpRequest request = HttpRequest.newBuilder()
            .DELETE()
            .uri(buildURI(key))
            .build();
        HttpResponse<Void> response = sendRequest(request, HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() != HttpCodes.ACCEPTED) {
            throw unexpectedHttpCodeException(response);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private URI buildURI(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        return baseUrl.resolve(HttpUtils.ENTITY_PATH + "?id=" + encodedKey);
    }

    private IOException unexpectedHttpCodeException(HttpResponse<?> response) {
        return new IOException("Unexpected http response code from KV service: " + response.statusCode());
    }

    private <T> HttpResponse<T> sendRequest(
        HttpRequest request,
        HttpResponse.BodyHandler<T> bodyHandler
    ) throws IOException {
        try {
            return client.send(request, bodyHandler);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted while waiting for response from kv service", e);
        }
    }
}
