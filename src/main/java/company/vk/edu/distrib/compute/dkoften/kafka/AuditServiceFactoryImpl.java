package company.vk.edu.distrib.compute.dkoften.kafka;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.AuditServiceFactory;

import java.io.IOException;

/**
 * Фабрика, создающая {@link AuditServiceImpl} — Kafka consumer для сбора аудит-событий.
 */
public final class AuditServiceFactoryImpl extends AuditServiceFactory {

    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) throws IOException {
        return new AuditServiceImpl(bootstrapServers, consumerGroupId);
    }
}

