package company.vk.edu.distrib.compute.dkoften.kafka;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

/**
 * Фабрика, создающая {@link AuditableKVServiceImpl} — KV-сервис с аудитом через Kafka.
 */
public final class AuditableKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new AuditableKVServiceImpl(port);
    }
}

