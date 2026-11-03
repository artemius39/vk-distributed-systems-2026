package company.vk.edu.distrib.compute.artemius39.kv.replication;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import company.vk.edu.distrib.compute.artemius39.kv.ShardedRemoteDao;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

@NullMarked
public class ReplicaConfig {
    private final int port;
    private final Lock mutex;
    private @Nullable List<URI> replicas;

    public ReplicaConfig(int port) {
        this.port = port;
        mutex = new ReentrantLock();
    }

    public List<URI> getReplicas() {
        mutex.lock();
        try {
            List<URI> configuredReplicas = replicas;
            if (configuredReplicas == null) {
                // Tests publish the cluster after constructing and starting the nodes.
                String cluster = System.getProperty("CLUSTER", Integer.toString(port));
                try {
                    configuredReplicas = Arrays.stream(cluster.split(",", -1))
                        .map(String::strip)
                        .mapToInt(Integer::parseInt)
                        .mapToObj(ReplicaConfig::replicaUri)
                        .toList();
                } catch (NumberFormatException e) {
                    throw new IllegalStateException("CLUSTER must contain comma-separated ports: '" + cluster + "'", e);
                }
                if (configuredReplicas.stream().distinct().count() != configuredReplicas.size()) {
                    throw new IllegalStateException("CLUSTER must contain distinct ports");
                }
                replicas = configuredReplicas;
            }
            return configuredReplicas;
        } finally {
            mutex.unlock();
        }
    }

    public int getReplicationFactor() {
        int clusterSize = getReplicas().size();
        int factor = Integer.parseInt(System.getProperty("REPLICATION_FACTOR", Integer.toString(clusterSize)));
        if (factor < 1 || factor > clusterSize) {
            throw new IllegalStateException("REPLICATION_FACTOR must be between 1 and the cluster size");
        }
        return factor;
    }

    public List<URI> getReplicas(String key) {
        List<URI> cluster = getReplicas();
        int factor = getReplicationFactor();
        int firstReplica = ShardedRemoteDao.pickShardIndex(key, cluster.size());
        List<URI> keyReplicas = new ArrayList<>(factor);
        for (int i = 0; i < factor; i++) {
            keyReplicas.add(cluster.get((firstReplica + i) % cluster.size()));
        }
        return keyReplicas;
    }

    private static URI replicaUri(int port) {
        if (port <= 0 || port >= 65536) {
            throw new IllegalStateException("CLUSTER port out of range: " + port);
        }
        return URI.create("http://localhost:" + port);
    }
}
