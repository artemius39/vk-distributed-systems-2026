package company.vk.edu.distrib.compute.artemius39.kv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.Objects;

import company.vk.edu.distrib.compute.Dao;

public class DiskDao implements Dao<byte[]> {
    private final Path directory;

    public DiskDao(Path directory) throws IOException {
        this.directory = Objects.requireNonNull(directory).toAbsolutePath();
        Files.createDirectories(this.directory);
    }

    private Path pathForKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key must not be null");
        }
        String fileName = Base64.getUrlEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return directory.resolve(fileName);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        try {
            return Files.readAllBytes(pathForKey(key));
        } catch (NoSuchFileException e) {
            throw new NoSuchElementException("Key not found: '" + key + "'", e);
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        Path target = pathForKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        Path temporary = Files.createTempFile(directory, "update-", ".tmp");
        try {
            Files.write(
                temporary, value,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC
            );
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }

    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        Files.deleteIfExists(pathForKey(key));
    }

    @Override
    public void close() {
        // каждая запись в файл сразу его закрывает
    }
}
