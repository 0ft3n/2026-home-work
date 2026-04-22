package company.vk.edu.distrib.compute.dkoften.storage;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class DaoImpl implements Dao<byte[]> {
    private final Path storage;

    private final Map<String, byte[]> loadedStorage = new ConcurrentHashMap<>();

    private final Logger logger = LoggerFactory.getLogger("dao");

    public DaoImpl(String path) {
        storage = Path.of(path);
        try {
            loadFromFile();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load data from file", e);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must be non-empty");
        }
        if (!loadedStorage.containsKey(key)) {
            throw new NoSuchElementException("Key not found");
        }
        return loadedStorage.get(key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must be non-empty");
        }
        loadedStorage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must be non-empty");
        }
        loadedStorage.remove(key);
    }

    @Override
    public void close() throws IOException {
        if (logger.isInfoEnabled()) {
            logger.info("Saving data to file");
        }
        saveToFile();
    }

    private void saveToFile() throws IOException {
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(storage)))) {
            out.writeInt(loadedStorage.size());
            for (Map.Entry<String, byte[]> entry : loadedStorage.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                byte[] value = entry.getValue();
                out.writeInt(value.length);
                out.write(value);
            }
        }
    }

    private void loadFromFile() throws IOException {
        if (!Files.exists(storage) || Files.size(storage) == 0) {
            return;
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(storage)))) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                readEntry(in);
            }

            if (logger.isInfoEnabled()) {
                logger.info("Loaded {} entries from file", loadedStorage.size());
            }
        }
    }

    private void readEntry(DataInputStream in) throws IOException {
        int keyLen = in.readInt();
        byte[] keyBytes = new byte[keyLen];
        in.readFully(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        int valueLen = in.readInt();
        byte[] value = new byte[valueLen];
        in.readFully(value);
        loadedStorage.put(key, value);
    }
}
