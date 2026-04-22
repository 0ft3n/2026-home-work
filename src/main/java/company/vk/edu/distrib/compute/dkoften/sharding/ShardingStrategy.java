package company.vk.edu.distrib.compute.dkoften.sharding;

import java.util.List;

@FunctionalInterface
public interface ShardingStrategy {
    String selectFor(List<String> endpoints, String key);
}
