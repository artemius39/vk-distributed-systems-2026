package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.Dao;

public class ShardedKVServiceFactoryImpl extends AbstractHttpServiceFactory<KVServiceImpl> {
    private static final int DEFAULT_WORKER_THREADS = 8;

    @Override
    protected KVServiceImpl doCreate(int port) throws IOException {
        int[] shardPorts = parseShardPorts();
        int workerThreads = parseWorkerThreads();
        List<Dao<byte[]>> shards = Arrays.stream(shardPorts)
            .mapToObj(ShardedKVServiceFactoryImpl::newShard)
            .toList();
        Dao<byte[]> dao = new ShardedRemoteDao<>(shards);
        ExecutorService executor = Executors.newFixedThreadPool(workerThreads);
        try {
            return new KVServiceImpl("localhost", port, new KVServiceHandler(dao), executor);
        } catch (IOException e) {
            executor.shutdownNow();
            try {
                dao.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    private static int parseWorkerThreads() {
        String configuredThreads = System.getenv("KV_WORKER_THREADS");
        int workerThreads = configuredThreads == null
            ? DEFAULT_WORKER_THREADS : Integer.parseInt(configuredThreads.strip());
        if (workerThreads <= 0) {
            throw new IllegalArgumentException("KV_WORKER_THREADS must be positive");
        }
        return workerThreads;
    }

    private static Dao<byte[]> newShard(int shardPort) {
        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();
        URI baseUrl = URI.create("http://localhost:" + shardPort);
        return new SingleNodeRemoteDao(client, baseUrl);
    }

    private static int[] parseShardPorts() {
        String ports = System.getenv("KV_SHARD_PORTS");
        if (ports == null || ports.isBlank()) {
            throw new IllegalArgumentException("must set KV_SHARD_PORTS");
        }
        return Arrays.stream(ports.split(","))
            .map(String::strip)
            .mapToInt(Integer::parseInt)
            .toArray();
    }
}
