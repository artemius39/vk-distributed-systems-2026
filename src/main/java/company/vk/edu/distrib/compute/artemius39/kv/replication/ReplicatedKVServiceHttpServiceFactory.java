package company.vk.edu.distrib.compute.artemius39.kv.replication;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.artemius39.kv.DiskDao;
import company.vk.edu.distrib.compute.artemius39.kv.KVServiceHandler;
import company.vk.edu.distrib.compute.kv.ReplicatedKVServiceTest;
import org.jspecify.annotations.NullMarked;

@NullMarked
@ReplicatedKVServiceTest
public class ReplicatedKVServiceHttpServiceFactory extends AbstractHttpServiceFactory<ReplicatedKVService> {
    @Override
    protected ReplicatedKVService doCreate(int port) throws IOException {
        Path dataRoot = Path.of(System.getProperty("user.home"), ".kv", "artemius39", Integer.toString(port));
        Path directory = dataRoot.resolve(Integer.toString(port));
        KVServiceHandler internalHandler = new KVServiceHandler(new DiskDao(directory), "/internal/entity");
        ReplicatedKVServiceHandler handler = new ReplicatedKVServiceHandler(
            HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(300))
                .build(),
            new ReplicaConfig(port)
        );
        try {
            return new ReplicatedKVService("localhost", port, handler, internalHandler);
        } catch (IOException e) {
            handler.close();
            try {
                internalHandler.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }
}
