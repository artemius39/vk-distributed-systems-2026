package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.nio.file.Path;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

@KVServiceTest
public class KVServiceFactoryImpl extends AbstractHttpServiceFactory<KVServiceImpl> {
    @Override
    protected KVServiceImpl doCreate(int port) throws IOException {
        Path dataRoot = Path.of(System.getProperty("user.home"), ".kv", "artemius39");
        Path directory = dataRoot.resolve(Integer.toString(port));
        return new KVServiceImpl("localhost", port, new KVServiceHandler(new DiskDao(directory)));
    }
}
