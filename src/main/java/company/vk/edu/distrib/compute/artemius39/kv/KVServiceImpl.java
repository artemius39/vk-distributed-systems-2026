package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;
import org.jspecify.annotations.Nullable;

public class KVServiceImpl implements KVService {
    private static final int SERVER_STOP_DELAY = 1;

    private final HttpServer server;
    private final KVServiceHandler handler;
    private final @Nullable ExecutorService executor;

    public KVServiceImpl(String host, int port, KVServiceHandler handler) throws IOException {
        this(host, port, handler, null);
    }

    public KVServiceImpl(
        String host, int port, KVServiceHandler handler, @Nullable ExecutorService executor
    ) throws IOException {
        this.server = new HttpServerBuilder()
            .host(host).port(port)
            .endpoint("GET", "/v0/status", handler::status)
            .endpoint("GET", "/v0/entity", handler::getByKey)
            .endpoint("PUT", "/v0/entity", handler::upsertByKey)
            .endpoint("DELETE", "/v0/entity", handler::deleteByKey)
            .build();
        if (executor != null) {
            server.setExecutor(executor);
        }
        this.handler = handler;
        this.executor = executor;
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        try {
            server.stop(SERVER_STOP_DELAY);
            if (executor != null) {
                executor.shutdownNow();
            }
            handler.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
