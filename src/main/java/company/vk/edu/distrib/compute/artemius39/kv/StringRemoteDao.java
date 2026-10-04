package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;

public class StringRemoteDao implements Dao<String> {
    private final Dao<byte[]> dao;
    private final KVService service;
    private boolean closed;

    public StringRemoteDao(Dao<byte[]> dao, KVService service) {
        this.dao = dao;
        this.service = service;
    }

    @Override
    public String get(String key) throws IOException {
        return new String(dao.get(key), StandardCharsets.UTF_8);
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        dao.upsert(key, value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void delete(String key) throws IOException {
        dao.delete(key);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            dao.close();
        } finally {
            service.stop();
        }
    }
}
