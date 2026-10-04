package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;

public class KVServiceImpl implements KVService {
    private static final int SERVER_STOP_DELAY = 1;

    private final HttpServer server;
    private final KVServiceHandler handler;

    public KVServiceImpl(String host, int port, KVServiceHandler handler) throws IOException {
        this.server = new HttpServerBuilder()
            .host(host).port(port)
            .endpoint("GET", "/v0/status", handler::status)
            .endpoint("GET", "/v0/entity", handler::getByKey)
            .endpoint("PUT", "/v0/entity", handler::upsertByKey)
            .endpoint("DELETE", "/v0/entity", handler::deleteByKey)
            .build();
        this.handler = handler;
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        try {
            server.stop(SERVER_STOP_DELAY);
            handler.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
