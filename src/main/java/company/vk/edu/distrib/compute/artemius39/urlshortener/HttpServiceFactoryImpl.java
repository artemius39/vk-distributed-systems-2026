package company.vk.edu.distrib.compute.artemius39.urlshortener;

import java.io.IOException;
import java.nio.file.Path;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerAuthTest;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerTest;

@UrlShortenerTest
@UrlShortenerAuthTest
public class HttpServiceFactoryImpl extends AbstractHttpServiceFactory<UrlShortenerServiceImpl> {
    @Override
    protected UrlShortenerServiceImpl doCreate(int port) throws IOException {
        Path dataRoot = Path.of(System.getProperty("artemius39.urlshortener.dataDir",
            Path.of(System.getProperty("user.home"), ".urlshortener", "artemius39").toString()));
        Path directory = dataRoot.resolve(Integer.toString(port));
        return new UrlShortenerServiceImpl(port,
            new PersistentDao(directory.resolve("links")), new PersistentDao(directory.resolve("users")));
    }
}
