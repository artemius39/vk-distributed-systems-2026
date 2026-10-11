package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import company.vk.edu.distrib.compute.Dao;
import org.jspecify.annotations.NullMarked;

@NullMarked
public class ShardedRemoteDao<T> implements Dao<T> {
    private final List<Dao<T>> shards;
    private final AtomicBoolean isClosed;

    public ShardedRemoteDao(List<Dao<T>> shards) {
        this.shards = shards;
        this.isClosed = new AtomicBoolean(false);
    }

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return pickShardForKey(key).get(key);
    }

    @Override
    public void upsert(String key, T value) throws IllegalArgumentException, IOException {
        pickShardForKey(key).upsert(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        pickShardForKey(key).delete(key);
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndExchange(false, true)) {
            IOException exception = null;
            for (Dao<T> shard : shards) {
                try {
                    shard.close();
                } catch (IOException e) {
                    if (exception == null) {
                        exception = e;
                    } else {
                        exception.addSuppressed(e);
                    }
                }
            }
            if (exception != null) {
                throw exception;
            }
        }
    }

    private Dao<T> pickShardForKey(String key) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 must be provided", e);
        }

        Dao<T> shardForKey = null;
        long maxWeight = Long.MIN_VALUE;
        for (int shardId = 0; shardId < shards.size(); shardId++) {
            long weight = randomWeight(md, shardId, key);
            if (weight > maxWeight) {
                maxWeight = weight;
                shardForKey = shards.get(shardId);
            }
        }
        if (shardForKey == null) {
            throw new IllegalStateException("Shard count must be non zero");
        }

        return shardForKey;
    }

    private long randomWeight(MessageDigest md, int shardId, String key) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(shardId);
        byte[] shardIdBytes = byteBuffer.array();
        md.update(shardIdBytes);

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        md.update(keyBytes);

        byte[] sha256Hash = md.digest();
        return ByteBuffer.wrap(sha256Hash).getLong();
    }
}
