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

/**
 * Реализация {@link ReplicatedService}: хранит {@code n} независимых реплик в памяти.
 *
 * <p>Особенности:
 * <ul>
 *   <li>Параллельный I/O — операции с репликами выполняются через виртуальные потоки.</li>
 *   <li>Last-Write-Wins на основе монотонного счётчика.</li>
 *   <li>Эндпоинты статистики:
 *     {@code GET /stats/replica/{id}} и {@code GET /stats/replica/{id}/access}.</li>
 * </ul>
 *
 * <p>Количество реплик задаётся системным свойством {@code dkoften.replicas} (по умолчанию 3).
 */
public final class ReplicatedKVServiceImpl implements ReplicatedService {

    private static final int DEFAULT_REPLICAS = Integer.getInteger("dkoften.replicas", 3);
    private static final Logger LOG = LoggerFactory.getLogger(ReplicatedKVServiceImpl.class);

    /** Монотонный счётчик — гарантирует строгий порядок записей в рамках одного JVM. */
    private static final AtomicLong CLOCK = new AtomicLong();

    private final int port;
    private final int numReplicas;
    private final HttpServer server;

    /** Пул виртуальных потоков для параллельных операций с репликами. */
    private final ExecutorService replicaExecutor = Executors.newVirtualThreadPerTaskExecutor();

    // -----------------------------------------------------------------------
    // Данные реплик
    // -----------------------------------------------------------------------

    @SuppressWarnings({"unchecked", "rawtypes"})
    private final Map<String, VersionedEntry>[] replicaData;
    private final AtomicBoolean[] replicaEnabled;

    // -----------------------------------------------------------------------
    // Статистика
    // -----------------------------------------------------------------------

    private final AtomicLong[] readCount;
    private final AtomicLong[] writeCount;
    private final AtomicLong[] deleteCount;

    // -----------------------------------------------------------------------
    // Внутренний тип
    // -----------------------------------------------------------------------

    private record VersionedEntry(byte[] value, long timestamp, boolean deleted) {
    }

    // -----------------------------------------------------------------------
    // Конструкторы
    // -----------------------------------------------------------------------

    ReplicatedKVServiceImpl(int port) {
        this(port, DEFAULT_REPLICAS);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    ReplicatedKVServiceImpl(int port, int numReplicas) {
        this.port = port;
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

    // -----------------------------------------------------------------------
    // HTTP: /v0/status
    // -----------------------------------------------------------------------

    private void handleStatus(HttpExchange exchange) {
        try (exchange) {
            exchange.sendResponseHeaders(200, 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // -----------------------------------------------------------------------
    // HTTP: /v0/entity
    // -----------------------------------------------------------------------

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

    private void handleRequest(HttpExchange exchange) throws IOException {
        String rawQuery = exchange.getRequestURI().getQuery();
        if (rawQuery == null) {
            sendEmpty(exchange, 400);
            return;
        }

        String id = null;
        int ack = 1;

        for (String param : rawQuery.split("&")) {
            if (param.startsWith("id=")) {
                id = param.substring(3);
            } else if (param.startsWith("ack=")) {
                try {
                    ack = Integer.parseInt(param.substring(4));
                } catch (NumberFormatException e) {
                    sendEmpty(exchange, 400);
                    return;
                }
            }
        }

        if (id == null || id.isEmpty()) {
            sendEmpty(exchange, 400);
            return;
        }

        if (ack < 1 || ack > numReplicas) {
            sendEmpty(exchange, 400);
            return;
        }

        switch (exchange.getRequestMethod()) {
            case "GET" -> handleGet(exchange, id, ack);
            case "PUT" -> handlePut(exchange, id, ack);
            case "DELETE" -> handleDelete(exchange, id, ack);
            default -> sendEmpty(exchange, 405);
        }
    }

    // -----------------------------------------------------------------------
    // GET — параллельное чтение из реплик
    // -----------------------------------------------------------------------

    private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
        List<CompletableFuture<VersionedEntry>> futures = new ArrayList<>(numReplicas);

        for (int i = 0; i < numReplicas; i++) {
            final int idx = i;
            if (!replicaEnabled[idx].get()) {
                futures.add(CompletableFuture.failedFuture(new IllegalStateException("disabled")));
                continue;
            }
            futures.add(CompletableFuture.supplyAsync(() -> {
                readCount[idx].incrementAndGet();
                return replicaData[idx].get(id); // null если не найдено
            }, replicaExecutor));
        }

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

    // -----------------------------------------------------------------------
    // PUT — параллельная запись в реплики
    // -----------------------------------------------------------------------

    private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
        byte[] value = exchange.getRequestBody().readAllBytes();
        long ts = CLOCK.incrementAndGet();

        List<CompletableFuture<Void>> futures = new ArrayList<>(numReplicas);
        for (int i = 0; i < numReplicas; i++) {
            final int idx = i;
            if (!replicaEnabled[idx].get()) {
                futures.add(CompletableFuture.failedFuture(new IllegalStateException("disabled")));
                continue;
            }
            futures.add(CompletableFuture.runAsync(() -> {
                replicaData[idx].merge(id, new VersionedEntry(value, ts, false),
                        (ex, cand) -> cand.timestamp() > ex.timestamp() ? cand : ex);
                writeCount[idx].incrementAndGet();
            }, replicaExecutor));
        }

        int confirmed = countSuccesses(futures);
        sendEmpty(exchange, confirmed >= ack ? 201 : 500);
    }

    // -----------------------------------------------------------------------
    // DELETE — параллельное удаление из реплик
    // -----------------------------------------------------------------------

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        long ts = CLOCK.incrementAndGet();

        List<CompletableFuture<Void>> futures = new ArrayList<>(numReplicas);
        for (int i = 0; i < numReplicas; i++) {
            final int idx = i;
            if (!replicaEnabled[idx].get()) {
                futures.add(CompletableFuture.failedFuture(new IllegalStateException("disabled")));
                continue;
            }
            futures.add(CompletableFuture.runAsync(() -> {
                replicaData[idx].merge(id, new VersionedEntry(null, ts, true),
                        (ex, cand) -> cand.timestamp() > ex.timestamp() ? cand : ex);
                deleteCount[idx].incrementAndGet();
            }, replicaExecutor));
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

    // -----------------------------------------------------------------------
    // HTTP: /stats/replica/{id}[/access]
    // -----------------------------------------------------------------------

    private void handleStats(HttpExchange exchange) {
        try (exchange) {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendEmpty(exchange, 405);
                return;
            }

            // Путь вида: /stats/replica/0  или  /stats/replica/0/access
            String path = exchange.getRequestURI().getPath();
            // Убираем ведущий /stats/replica/
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

            String body;
            if (accessMode) {
                body = buildAccessJson(replicaId);
            } else {
                body = buildInfoJson(replicaId);
            }

            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Формирует JSON с информацией о реплике: кол-во ключей, размер данных, статус.
     *
     * <p>Пример: {@code {"replicaId":0,"keys":42,"dataBytes":4096,"enabled":true}}
     */
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

    /**
     * Формирует JSON с частотой обращений к реплике: чтения, записи, удаления.
     *
     * <p>Пример: {@code {"replicaId":0,"reads":100,"writes":50,"deletes":5}}
     */
    private String buildAccessJson(int replicaId) {
        return String.format(
                "{\"replicaId\":%d,\"reads\":%d,\"writes\":%d,\"deletes\":%d}",
                replicaId,
                readCount[replicaId].get(),
                writeCount[replicaId].get(),
                deleteCount[replicaId].get());
    }

    // -----------------------------------------------------------------------
    // Утилиты
    // -----------------------------------------------------------------------

    private static void sendEmpty(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, 0);
    }

    // -----------------------------------------------------------------------
    // ReplicatedService
    // -----------------------------------------------------------------------

    @Override
    public int port() {
        return port;
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

    // -----------------------------------------------------------------------
    // KVService
    // -----------------------------------------------------------------------

    @Override
    public void start() {
        server.start();
        LOG.info("ReplicatedKVService started on port {} with {} replicas", port, numReplicas);
    }

    @Override
    public void stop() {
        server.stop(0);
        replicaExecutor.close();
        LOG.info("ReplicatedKVService stopped");
    }

    // -----------------------------------------------------------------------
    // Фабрика
    // -----------------------------------------------------------------------

    public static final class ReplicatedFactory extends KVServiceFactory {

        @Override
        protected KVService doCreate(int port) {
            return new ReplicatedKVServiceImpl(port);
        }
    }
}



