package company.vk.edu.distrib.compute.dkoften.sharding.strategies;

import company.vk.edu.distrib.compute.dkoften.sharding.ShardingStrategy;

import java.util.List;
import java.util.Objects;

public final class ModuloShardingStrategy implements ShardingStrategy {
    @Override
    public String selectFor(List<String> endpoints, String key) {
        Objects.requireNonNull(key, "key must not be null");
        int index = Math.floorMod(key.hashCode(), endpoints.size());
        return endpoints.get(index);
    }
}
