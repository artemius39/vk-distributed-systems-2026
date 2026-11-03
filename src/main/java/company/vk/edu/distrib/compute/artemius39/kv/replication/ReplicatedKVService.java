package company.vk.edu.distrib.compute.artemius39.kv.replication;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.artemius39.kv.HttpServerBuilder;
import company.vk.edu.distrib.compute.artemius39.kv.KVServiceHandler;
import company.vk.edu.distrib.compute.kv.KVService;
import org.jspecify.annotations.NullMarked;

@NullMarked
public class ReplicatedKVService implements KVService {
    private static final int SERVER_STOP_DELAY = 0;

    private final HttpServer server;
    private final ReplicatedKVServiceHandler handler;
    private final KVServiceHandler internalHandler;
    private final ExecutorService executor;
    private final AtomicBoolean isStopped = new AtomicBoolean();

    public ReplicatedKVService(
        String host,
        int port,
        ReplicatedKVServiceHandler handler,
        KVServiceHandler internalHandler
    ) throws IOException {
        this.server = new HttpServerBuilder()
            .host(host).port(port)
            .endpoint("GET", "/v0/status", handler::status)
            .endpoint("GET", "/v0/entity", handler::getByKey)
            .endpoint("PUT", "/v0/entity", handler::upsertByKey)
            .endpoint("DELETE", "/v0/entity", handler::deleteByKey)
            .endpoint("GET", "/internal/entity", internalHandler::getByKey)
            .endpoint("PUT", "/internal/entity", internalHandler::upsertByKey)
            .endpoint("DELETE", "/internal/entity", internalHandler::deleteByKey)
            .build();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        this.handler = handler;
        this.internalHandler = internalHandler;
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        if (!isStopped.compareAndSet(false, true)) {
            return;
        }
        try {
            server.stop(SERVER_STOP_DELAY);
            executor.shutdownNow();
            handler.close();
            internalHandler.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
