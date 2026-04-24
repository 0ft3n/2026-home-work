package company.vk.edu.distrib.compute.dkoften;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.ReplicatedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("PMD.GodClass")
public final class ReplicatedKVServiceImpl implements ReplicatedService {

    private static final int DEFAULT_REPLICAS = Integer.getInteger("dkoften.replicas", 3);
    private static final Logger LOG = LoggerFactory.getLogger(ReplicatedKVServiceImpl.class);

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String DISABLED_MSG = "disabled";

    private static final AtomicLong CLOCK = new AtomicLong();

    private static final CompletableFuture<VersionedEntry> DISABLED_FUTURE_READ =
            CompletableFuture.failedFuture(new IllegalStateException(DISABLED_MSG));
    private static final CompletableFuture<Void> DISABLED_FUTURE_WRITE =
            CompletableFuture.failedFuture(new IllegalStateException(DISABLED_MSG));

    private final int serverPort;
    private final int numReplicas;
    private final HttpServer server;

    private final ExecutorService replicaExecutor = Executors.newVirtualThreadPerTaskExecutor();

    @SuppressWarnings({"unchecked", "rawtypes"})
    private final Map<String, VersionedEntry>[] replicaData;
    private final AtomicBoolean[] replicaEnabled;

    private final AtomicLong[] readCount;
    private final AtomicLong[] writeCount;
    private final AtomicLong[] deleteCount;

    private record VersionedEntry(byte[] value, long timestamp, boolean deleted) {
    }

    ReplicatedKVServiceImpl(int port) {
        this(port, DEFAULT_REPLICAS);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    ReplicatedKVServiceImpl(int port, int numReplicas) {
        this.serverPort = port;
        this.numReplicas = numReplicas;
        this.replicaData = new Map[numReplicas];
        this.replicaEnabled = new AtomicBoolean[numReplicas];
        this.readCount = new AtomicLong[numReplicas];
        this.writeCount = new AtomicLong[numReplicas];
        this.deleteCount = new AtomicLong[numReplicas];

        for (int i = 0; i < numReplicas; i++) {
            replicaData[i] = new ConcurrentHashMap<>();
            replicaEnabled[i] = new AtomicBoolean(true);
            readCount[i] = new AtomicLong();
            writeCount[i] = new AtomicLong();
            deleteCount[i] = new AtomicLong();
        }

        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/v0/entity", this::handleEntity);
            server.createContext("/v0/status", this::handleStatus);
            server.createContext("/stats/replica/", this::handleStats);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void handleStatus(HttpExchange exchange) {
        try (exchange) {
            exchange.sendResponseHeaders(200, 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void handleEntity(HttpExchange exchange) {
        try (exchange) {
            try {
                handleRequest(exchange);
            } catch (IllegalArgumentException e) {
                sendEmpty(exchange, 400);
            } catch (Exception e) {
                LOG.error("Unexpected error", e);
                sendEmpty(exchange, 500);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String parseId(String rawQuery) {
        for (String param : rawQuery.split("&")) {
            if (param.startsWith("id=")) {
                String id = param.substring(3);
                return id.isEmpty() ? null : id;
            }
        }
        return null;
    }

    private int parseAck(String rawQuery) {
        for (String param : rawQuery.split("&")) {
            if (param.startsWith("ack=")) {
                try {
                    return Integer.parseInt(param.substring(4));
                } catch (NumberFormatException e) {
                    return -1;
                }
            }
        }
        return 1;
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        String rawQuery = exchange.getRequestURI().getQuery();
        if (rawQuery == null) {
            sendEmpty(exchange, 400);
            return;
        }

        String id = parseId(rawQuery);
        if (id == null) {
            sendEmpty(exchange, 400);
            return;
        }

        int ack = parseAck(rawQuery);
        if (ack < 1 || ack > numReplicas) {
            sendEmpty(exchange, 400);
            return;
        }

        dispatchMethod(exchange, id, ack);
    }

    private void dispatchMethod(HttpExchange exchange, String id, int ack) throws IOException {
        switch (exchange.getRequestMethod()) {
            case METHOD_GET -> handleGet(exchange, id, ack);
            case METHOD_PUT -> handlePut(exchange, id, ack);
            case METHOD_DELETE -> handleDelete(exchange, id, ack);
            default -> sendEmpty(exchange, 405);
        }
    }

    private List<CompletableFuture<VersionedEntry>> buildReadFutures(String id) {
        List<CompletableFuture<VersionedEntry>> futures = new ArrayList<>(numReplicas);
        for (int i = 0; i < numReplicas; i++) {
            final int idx = i;
            if (replicaEnabled[idx].get()) {
                futures.add(CompletableFuture.supplyAsync(() -> {
                    readCount[idx].incrementAndGet();
                    return replicaData[idx].get(id);
                }, replicaExecutor));
            } else {
                futures.add(DISABLED_FUTURE_READ);
            }
        }
        return futures;
    }

    private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
        List<CompletableFuture<VersionedEntry>> futures = buildReadFutures(id);

        int confirmed = 0;
        VersionedEntry best = null;
        for (CompletableFuture<VersionedEntry> f : futures) {
            try {
                VersionedEntry entry = f.join();
                confirmed++;
                if (entry != null && (best == null || entry.timestamp() > best.timestamp())) {
                    best = entry;
                }
            } catch (Exception ignored) {
                // реплика недоступна — не считается
            }
        }

        if (confirmed < ack) {
            sendEmpty(exchange, 500);
            return;
        }

        if (best == null || best.deleted()) {
            sendEmpty(exchange, 404);
            return;
        }

        byte[] value = best.value();
        exchange.sendResponseHeaders(200, value.length);
        exchange.getResponseBody().write(value);
    }

    private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
        byte[] value = exchange.getRequestBody().readAllBytes();
        long ts = CLOCK.incrementAndGet();
        VersionedEntry newEntry = new VersionedEntry(value, ts, false);

        List<CompletableFuture<Void>> futures = new ArrayList<>(numReplicas);
        for (int i = 0; i < numReplicas; i++) {
            final int idx = i;
            if (replicaEnabled[idx].get()) {
                futures.add(CompletableFuture.runAsync(() -> {
                    replicaData[idx].merge(id, newEntry,
                            (ex, cand) -> cand.timestamp() > ex.timestamp() ? cand : ex);
                    writeCount[idx].incrementAndGet();
                }, replicaExecutor));
            } else {
                futures.add(DISABLED_FUTURE_WRITE);
            }
        }

        int confirmed = countSuccesses(futures);
        sendEmpty(exchange, confirmed >= ack ? 201 : 500);
    }

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        long ts = CLOCK.incrementAndGet();
        VersionedEntry tombstone = new VersionedEntry(null, ts, true);

        List<CompletableFuture<Void>> futures = new ArrayList<>(numReplicas);
        for (int i = 0; i < numReplicas; i++) {
            final int idx = i;
            if (replicaEnabled[idx].get()) {
                futures.add(CompletableFuture.runAsync(() -> {
                    replicaData[idx].merge(id, tombstone,
                            (ex, cand) -> cand.timestamp() > ex.timestamp() ? cand : ex);
                    deleteCount[idx].incrementAndGet();
                }, replicaExecutor));
            } else {
                futures.add(DISABLED_FUTURE_WRITE);
            }
        }

        int confirmed = countSuccesses(futures);
        sendEmpty(exchange, confirmed >= ack ? 202 : 500);
    }

    private static int countSuccesses(List<CompletableFuture<Void>> futures) {
        int count = 0;
        for (CompletableFuture<Void> f : futures) {
            try {
                f.join();
                count++;
            } catch (Exception ignored) {
                // реплика недоступна
            }
        }
        return count;
    }

    private void handleStats(HttpExchange exchange) {
        try (exchange) {
            if (!METHOD_GET.equals(exchange.getRequestMethod())) {
                sendEmpty(exchange, 405);
                return;
            }

            String path = exchange.getRequestURI().getPath();
            String tail = path.replaceFirst("^/stats/replica/", "");
            boolean accessMode = tail.endsWith("/access");
            String idPart = accessMode ? tail.substring(0, tail.length() - "/access".length()) : tail;

            int replicaId;
            try {
                replicaId = Integer.parseInt(idPart);
            } catch (NumberFormatException e) {
                sendEmpty(exchange, 400);
                return;
            }

            if (replicaId < 0 || replicaId >= numReplicas) {
                sendEmpty(exchange, 404);
                return;
            }

            String body = accessMode ? buildAccessJson(replicaId) : buildInfoJson(replicaId);
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String buildInfoJson(int replicaId) {
        Map<String, VersionedEntry> data = replicaData[replicaId];
        long keyCount = data.values().stream().filter(e -> !e.deleted()).count();
        long dataBytes = data.values().stream()
                .filter(e -> !e.deleted() && e.value() != null)
                .mapToLong(e -> e.value().length)
                .sum();
        boolean enabled = replicaEnabled[replicaId].get();
        return String.format(
                "{\"replicaId\":%d,\"keys\":%d,\"dataBytes\":%d,\"enabled\":%b}",
                replicaId, keyCount, dataBytes, enabled);
    }

    private String buildAccessJson(int replicaId) {
        return String.format(
                "{\"replicaId\":%d,\"reads\":%d,\"writes\":%d,\"deletes\":%d}",
                replicaId,
                readCount[replicaId].get(),
                writeCount[replicaId].get(),
                deleteCount[replicaId].get());
    }

    private static void sendEmpty(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, 0);
    }

    @Override
    public int port() {
        return serverPort;
    }

    @Override
    public int numberOfReplicas() {
        return numReplicas;
    }

    @Override
    public void disableReplica(int nodeId) {
        if (nodeId >= 0 && nodeId < numReplicas) {
            replicaEnabled[nodeId].set(false);
            LOG.debug("Replica {} disabled", nodeId);
        }
    }

    @Override
    public void enableReplica(int nodeId) {
        if (nodeId >= 0 && nodeId < numReplicas) {
            replicaEnabled[nodeId].set(true);
            LOG.debug("Replica {} enabled", nodeId);
        }
    }

    @Override
    public void start() {
        server.start();
        LOG.info("ReplicatedKVService started on port {} with {} replicas", serverPort, numReplicas);
    }

    @Override
    public void stop() {
        server.stop(0);
        replicaExecutor.close();
        LOG.info("ReplicatedKVService stopped");
    }

    public static final class ReplicatedFactory extends KVServiceFactory {

        @Override
        protected KVService doCreate(int port) {
            return new ReplicatedKVServiceImpl(port);
        }
    }
}



