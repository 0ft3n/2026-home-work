package company.vk.edu.distrib.compute.dkoften.kafka;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Kafka consumer, накапливающий аудит-события из топика {@code audit}.
 *
 * <p>Использует consumer group для отслеживания позиции в топике:
 * перезапуск сервиса с тем же {@code consumerGroupId} не приводит к повторному
 * прочтению уже обработанных событий.
 */
public final class AuditServiceImpl implements AuditService {

    private static final Logger LOG = LoggerFactory.getLogger(AuditServiceImpl.class);
    private static final String AUDIT_TOPIC = "audit";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final String servers;
    private final String groupId;
    private final List<AuditEvent> events = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicReference<KafkaConsumer<String, String>> consumer = new AtomicReference<>();
    private final AtomicReference<Thread> consumerThread = new AtomicReference<>();

    /**
     * Создаёт экземпляр сервиса аудита.
     *
     * @param servers Kafka bootstrap-servers
     * @param groupId идентификатор consumer-группы; используется для хранения смещений
     */
    public AuditServiceImpl(String servers, String groupId) {
        this.servers = servers;
        this.groupId = groupId;
    }

    @Override
    public void start() {
        KafkaConsumer<String, String> kafkaConsumer = buildConsumer(servers, groupId);
        consumer.set(kafkaConsumer);
        kafkaConsumer.subscribe(List.of(AUDIT_TOPIC));
        running.set(true);
        Thread thread = Thread.ofVirtual().name("audit-consumer-" + groupId).start(this::consumeLoop);
        consumerThread.set(thread);
    }

    @Override
    public void stop() {
        running.set(false);
        KafkaConsumer<String, String> kafkaConsumer = consumer.get();
        if (kafkaConsumer != null) {
            kafkaConsumer.wakeup(); // thread-safe – unblocks poll()
        }
        Thread thread = consumerThread.get();
        if (thread != null) {
            try {
                thread.join(3_000L);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return new ArrayList<>(events);
    }

    private void consumeLoop() {
        try (KafkaConsumer<String, String> kafkaConsumer = consumer.get()) {
            while (running.get()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, String> record : records) {
                    AuditEvent event = decode(record.value());
                    if (event != null) {
                        events.add(event);
                    }
                }
            }
        } catch (WakeupException ex) {
            LOG.debug("Consumer woken up – shutting down", ex);
        }
    }

    /**
     * Декодирует строку формата {@code method|id|timestamp} в {@link AuditEvent}.
     */
    private static AuditEvent decode(String value) {
        if (value == null) {
            return null;
        }
        String[] parts = value.split("\\|", 3);
        if (parts.length != 3) {
            return null;
        }
        try {
            return new AuditEvent(parts[0], parts[1], Long.parseLong(parts[2]));
        } catch (NumberFormatException ex) {
            LOG.warn("Malformed audit record: {}", value, ex);
            return null;
        }
    }

    private static KafkaConsumer<String, String> buildConsumer(String servers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "200");
        return new KafkaConsumer<>(props);
    }
}


