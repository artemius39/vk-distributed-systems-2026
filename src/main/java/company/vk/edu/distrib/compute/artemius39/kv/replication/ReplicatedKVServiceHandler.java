package company.vk.edu.distrib.compute.artemius39.kv.replication;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.artemius39.kv.HttpCodes;
import company.vk.edu.distrib.compute.artemius39.kv.HttpUtils;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicatedKVServiceHandler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedKVServiceHandler.class);
    private static final Duration REPLICA_TIMEOUT = Duration.ofSeconds(1);

    private final HttpClient replicaClient;
    private final ReplicaConfig replicaConfig;

    public ReplicatedKVServiceHandler(HttpClient replicaClient, ReplicaConfig replicaConfig) {
        this.replicaClient = replicaClient;
        this.replicaConfig = replicaConfig;
    }

    public void status(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(HttpCodes.OK, HttpUtils.EMPTY_RESPONSE_LENGTH);
    }

    public void getByKey(HttpExchange exchange) throws IOException {
        Map<String, List<String>> params = HttpUtils.parseQueryParams(exchange);
        String id = parseId(params);
        Integer ack = parseAck(params);
        if (id == null || ack == null) {
            HttpUtils.sendEmpty(exchange, HttpCodes.BAD_REQUEST);
            return;
        }

        List<HttpResponse<byte[]>> responses = queryReplicas(
            id,
            ack,
            HttpResponse.BodyHandlers.ofByteArray(),
            response -> response.statusCode() == HttpCodes.OK || response.statusCode() == HttpCodes.NOT_FOUND,
            uri -> HttpRequest.newBuilder()
                .GET()
                .uri(replicaUri(uri, id))
                .timeout(REPLICA_TIMEOUT)
                .build()
        );
        if (responses.size() < ack) {
            HttpUtils.sendEmpty(exchange, HttpCodes.SERVICE_UNAVAILABLE);
            return;
        }
        byte[] result = responses.stream()
            .filter(response -> response.statusCode() == HttpCodes.OK)
            .findAny()
            .map(HttpResponse::body)
            .orElse(null);
        if (result == null) {
            HttpUtils.sendEmpty(exchange, HttpCodes.NOT_FOUND);
        } else {
            exchange.sendResponseHeaders(
                HttpCodes.OK, result.length == 0 ? HttpUtils.EMPTY_RESPONSE_LENGTH : result.length
            );
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(result);
            }
        }
    }

    private @Nullable String parseId(Map<String, List<String>> params) {
        List<String> ids = params.get("id");
        if (ids == null || ids.size() != 1 || ids.getFirst().isEmpty()) {
            return null;
        }
        return ids.getFirst();
    }

    private @Nullable Integer parseAck(Map<String, List<String>> params) {
        List<String> acks = params.get("ack");
        if (acks == null) {
            return 1;
        }
        if (acks.size() != 1) {
            return null;
        }
        String ackStr = acks.getFirst();
        try {
            int ack = Integer.parseInt(ackStr);
            return ack >= 1 && ack <= replicaConfig.getReplicationFactor() ? ack : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private <T> List<HttpResponse<T>> queryReplicas(
        String key,
        int ack,
        HttpResponse.BodyHandler<T> bodyHandler,
        Predicate<HttpResponse<T>> responsePredicate,
        Function<URI, HttpRequest> requestBuilder
    ) {
        List<URI> replicas = replicaConfig.getReplicas(key);
        BlockingQueue<Optional<HttpResponse<T>>> responsesQueue = new LinkedBlockingQueue<>(replicas.size());
        long deadline = System.nanoTime() + REPLICA_TIMEOUT.toNanos();
        for (URI replicaURI : replicas) {
            HttpRequest request = requestBuilder.apply(replicaURI);
            var responseFuture = replicaClient.sendAsync(request, bodyHandler);
            responseFuture.whenComplete((response, e) -> responsesQueue.add(Optional.ofNullable(response)));
        }

        List<HttpResponse<T>> responses = new ArrayList<>(replicas.size());
        int remaining = replicas.size();
        while (responses.size() < ack && responses.size() + remaining >= ack) {
            Optional<HttpResponse<T>> completed;
            try {
                completed = responsesQueue.poll(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted, aborting request handling", e);
                break;
            }
            if (completed == null) {
                log.warn("Replica queries timed out");
                break;
            }
            remaining--;
            completed.filter(responsePredicate).ifPresent(responses::add);
        }
        return responses;
    }

    public void upsertByKey(HttpExchange exchange) throws IOException {
        Map<String, List<String>> params = HttpUtils.parseQueryParams(exchange);
        String id = parseId(params);
        Integer ack = parseAck(params);
        if (id == null || ack == null) {
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

        List<HttpResponse<Void>> responses = queryReplicas(
            id,
            ack,
            HttpResponse.BodyHandlers.discarding(),
            response -> response.statusCode() == HttpCodes.CREATED,
            uri -> HttpRequest.newBuilder()
                .PUT(HttpRequest.BodyPublishers.ofByteArray(value))
                .uri(replicaUri(uri, id))
                .timeout(REPLICA_TIMEOUT)
                .build()
        );
        HttpUtils.sendEmpty(exchange, responses.size() >= ack ? HttpCodes.CREATED : HttpCodes.SERVICE_UNAVAILABLE);
    }

    public void deleteByKey(HttpExchange exchange) throws IOException {
        Map<String, List<String>> params = HttpUtils.parseQueryParams(exchange);
        String id = parseId(params);
        Integer ack = parseAck(params);
        if (id == null || ack == null) {
            HttpUtils.sendEmpty(exchange, HttpCodes.BAD_REQUEST);
            return;
        }

        List<HttpResponse<Void>> responses = queryReplicas(
            id,
            ack,
            HttpResponse.BodyHandlers.discarding(),
            response -> response.statusCode() == HttpCodes.ACCEPTED,
            replicaUri -> HttpRequest.newBuilder()
                .DELETE()
                .uri(replicaUri(replicaUri, id))
                .timeout(REPLICA_TIMEOUT)
                .build()
        );
        HttpUtils.sendEmpty(exchange, responses.size() >= ack ? HttpCodes.ACCEPTED : HttpCodes.SERVICE_UNAVAILABLE);
    }

    private URI replicaUri(URI baseUri, String key) {
        return baseUri.resolve("/internal/entity?id=" + URLEncoder.encode(key, StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
        replicaClient.close();
    }
}
