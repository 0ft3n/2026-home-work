package company.vk.edu.distrib.compute.dkoften.sharding;

import company.vk.edu.distrib.compute.dkoften.sharding.strategies.ModuloShardingStrategy;
import company.vk.edu.distrib.compute.dkoften.sharding.strategies.RendezvousShardingStrategy;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

public final class ShardingBalancer {
    private static final String STRATEGY_PROPERTY = "dk.sharding.strategy";

    public enum StrategyType {
        MODULO,
        RENDEZVOUS
    }

    private final ShardingStrategy strategy;

    public ShardingBalancer() {
        String strategyName = System.getProperty(STRATEGY_PROPERTY, "RENDEZVOUS").toUpperCase(Locale.ROOT);
        StrategyType strategyType = StrategyType.valueOf(strategyName);
        this.strategy = createStrategy(strategyType);
    }

    public String selectFor(List<String> endpoints, String key) {
        return strategy.selectFor(endpoints, key);
    }

    private static ShardingStrategy createStrategy(StrategyType strategyType) {
        Objects.requireNonNull(strategyType, "strategyType must not be null");
        return switch (strategyType) {
            case MODULO -> new ModuloShardingStrategy();
            case RENDEZVOUS -> new RendezvousShardingStrategy();
        };
    }
}
