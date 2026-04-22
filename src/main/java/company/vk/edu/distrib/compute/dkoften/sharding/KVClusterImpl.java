package company.vk.edu.distrib.compute.dkoften.sharding;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.dkoften.KVServiceImpl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class KVClusterImpl implements KVCluster {
    private final Map<String, KVService> nodes;
    private final Map<String, Integer> endpointPorts;
    private final KVServiceImpl.Factory factory = new KVServiceImpl.Factory();

    KVClusterImpl(List<Integer> ports) {
        nodes = new HashMap<>();
        endpointPorts = new HashMap<>();
        for (var port : ports) {
            var endpoint = "http://localhost:" + port;
            endpointPorts.put(endpoint, port);
            try {
                nodes.put(endpoint, factory.create(port, this));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void start() {
        for (var node : nodes.entrySet()) {
            node.getValue().start();
        }
    }

    @Override
    public void start(String endpoint) {
        // Recreate service to allow restart after stop (HttpServer cannot be restarted)
        Integer port = endpointPorts.get(endpoint);
        if (port != null) {
            try {
                KVService service = factory.create(port, this);
                nodes.put(endpoint, service);
                service.start();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void stop() {
        for (var node: nodes.entrySet()) {
            node.getValue().stop();
        }
    }

    @Override
    public void stop(String endpoint) {
        var node = nodes.get(endpoint);
        if (node != null) {
            node.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return nodes.keySet().stream().toList();
    }

    public static class Factory extends KVClusterFactory {
        @Override
        protected KVCluster doCreate(List<Integer> ports) {
            return new KVClusterImpl(ports);
        }
    }
}
