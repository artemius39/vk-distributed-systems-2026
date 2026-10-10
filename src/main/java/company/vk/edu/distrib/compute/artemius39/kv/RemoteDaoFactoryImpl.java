package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public class RemoteDaoFactoryImpl implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length != 1) {
            throw new IllegalArgumentException("Expected exactly one KV service port");
        }
        int port = ports[0];

        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();
        RemoteDao dao = new RemoteDao(client, URI.create("http://localhost:" + port));
        return new StringRemoteDao(dao);
    }
}
