package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import org.jspecify.annotations.NullMarked;

@ClusterDaoFactoryTest
@NullMarked
public class ShardedRemoteDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) throws IOException {
        List<Dao<String>> shards = IntStream.of(ports)
            .mapToObj(SingleNodeRemoteDaoFactory::createOne)
            .toList();
        return new ShardedRemoteDao<>(shards);
    }
}
