package company.vk.edu.distrib.compute.dkoften.kafka;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditableKVService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * KV-сервис с HTTP API и аудитом операций через Apache Kafka.
 *
 * <p>Каждый запрос к {@code /v0/entity} публикует {@link company.vk.edu.distrib.compute.AuditEvent}
 * в Kafka-топик {@value #AUDIT_TOPIC}. Поддерживаются синхронный и асинхронный режимы отправки.
 */
public final class AuditableKVServiceImpl implements AuditableKVService {

    private static final Logger LOG = LoggerFactory.getLogger(AuditableKVServiceImpl.class);
    private static final String AUDIT_TOPIC = "audit";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private final int port;
    private final Map<String, byte[]> store = new ConcurrentHashMap<>();
    private final AtomicBoolean asyncEnabled = new AtomicBoolean();
    private final AtomicReference<String> bootstrapServers = new AtomicReference<>();
    private final AtomicReference<KafkaProducer<String, String>> producer = new AtomicReference<>();
    private final AtomicReference<HttpServer> httpServer = new AtomicReference<>();

    /**
     * Создаёт сервис, привязанный к указанному порту.
     *
     * @param port HTTP-порт для прослушивания
     */
    public AuditableKVServiceImpl(int port) {
        this.port = port;
    }

    /**
     * Возвращает HTTP-порт, на котором слушает сервис.
     *
     * @return номер порта
     */
    public int port() {
        return port;
    }

    @Override
    public void setBootstrapServers(String servers) {
        bootstrapServers.set(servers);
    }

    @Override
    public void setAsync(boolean enabled) {
        asyncEnabled.set(enabled);
    }

    @Override
    public void start() {
        String servers = bootstrapServers.get();
        if (servers != null) {
            producer.set(buildProducer(servers));
        }
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/v0/entity", this::handleEntity);
            server.createContext("/v0/status", this::handleStatus);
            server.start();
            httpServer.set(server);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void stop() {
        HttpServer server = httpServer.get();
        if (server != null) {
            server.stop(0);
        }
        KafkaProducer<String, String> prod = producer.get();
        if (prod != null) {
            prod.close();
        }
    }

    private void handleStatus(HttpExchange exchange) {
        try (exchange) {
            exchange.sendResponseHeaders(200, 0);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private void handleEntity(HttpExchange exchange) {
        try (exchange) {
            String rawQuery = exchange.getRequestURI().getQuery();
            String id = parseId(rawQuery);
            if (id == null) {
                exchange.sendResponseHeaders(400, 0);
                return;
            }
            String method = exchange.getRequestMethod();
            long timestamp = System.currentTimeMillis();
            dispatch(exchange, method, id);
            publishAudit(method, id, timestamp);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private void dispatch(HttpExchange exchange, String method, String id) throws IOException {
        switch (method) {
            case METHOD_GET -> {
                byte[] value = store.get(id);
                if (value == null) {
                    exchange.sendResponseHeaders(404, 0);
                } else {
                    exchange.sendResponseHeaders(200, value.length);
                    exchange.getResponseBody().write(value);
                }
            }
            case METHOD_PUT -> {
                byte[] body = exchange.getRequestBody().readAllBytes();
                store.put(id, body);
                exchange.sendResponseHeaders(201, 0);
            }
            case METHOD_DELETE -> {
                store.remove(id);
                exchange.sendResponseHeaders(202, 0);
            }
            default -> exchange.sendResponseHeaders(405, 0);
        }
    }

    private void publishAudit(String method, String id, long timestamp) {
        KafkaProducer<String, String> prod = producer.get();
        if (prod == null) {
            return;
        }
        String payload = method + "|" + id + "|" + timestamp;
        ProducerRecord<String, String> record = new ProducerRecord<>(AUDIT_TOPIC, id, payload);
        try {
            if (asyncEnabled.get()) {
                prod.send(record);
            } else {
                prod.send(record).get();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException ex) {
            LOG.warn("Failed to send audit event synchronously", ex);
        }
    }

    private static String parseId(String rawQuery) {
        if (rawQuery == null) {
            return null;
        }
        for (String param : rawQuery.split("&")) {
            if (param.startsWith("id=")) {
                String id = param.substring(3);
                return id.isEmpty() ? null : id;
            }
        }
        return null;
    }

    private static KafkaProducer<String, String> buildProducer(String servers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer<>(props);
    }
}


