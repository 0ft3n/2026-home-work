package company.vk.edu.distrib.compute.dkoften.sharding.strategies;

import company.vk.edu.distrib.compute.dkoften.sharding.ShardingStrategy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public final class RendezvousShardingStrategy implements ShardingStrategy {
    private static final long FNV_64_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV_64_PRIME = 0x100000001b3L;

    @Override
    public String selectFor(List<String> endpoints, String key) {
        Objects.requireNonNull(key, "key must not be null");

        String selected = null;
        long bestScore = 0;
        for (String endpoint : endpoints) {
            long score = score(key, endpoint);
            if (selected == null || Long.compareUnsigned(score, bestScore) > 0) {
                selected = endpoint;
                bestScore = score;
            }
        }

        return selected;
    }

    private static long score(String key, String endpoint) {
        long hash = FNV_64_OFFSET_BASIS;
        hash = updateHash(hash, key.getBytes(StandardCharsets.UTF_8));
        hash = updateHash(hash, new byte[]{0});
        return updateHash(hash, endpoint.getBytes(StandardCharsets.UTF_8));
    }

    private static long updateHash(long hash, byte[] bytes) {
        long result = hash;
        for (byte value : bytes) {
            result ^= value & 0xffL;
            result *= FNV_64_PRIME;
        }
        return result;
    }
}
