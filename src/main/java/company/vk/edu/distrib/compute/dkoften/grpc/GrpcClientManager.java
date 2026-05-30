package company.vk.edu.distrib.compute.dkoften.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GrpcClientManager {
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Logger logger = LoggerFactory.getLogger("grpc");

    public KVServiceGrpcGrpc.KVServiceGrpcBlockingStub getStub(String endpoint) {
        String key = extractGrpcAddress(endpoint);
        ManagedChannel channel = channels.computeIfAbsent(key, this::createChannel);
        return KVServiceGrpcGrpc.newBlockingStub(channel);
    }

    private ManagedChannel createChannel(String endpoint) {
        String[] parts = endpoint.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        if (logger.isDebugEnabled()) {
            logger.debug("Creating gRPC channel to {}:{}", host, port);
        }

        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
    }

    private String extractGrpcAddress(String endpoint) {
        try {
            if (endpoint.contains("?")) {
                String[] parts = endpoint.split("\\?");
                String[] params = parts[1].split("&");
                for (String param : params) {
                    if (param.startsWith("grpcPort=")) {
                        String host = parts[0].split(":")[0];
                        String port = param.substring("grpcPort=".length());
                        return host + ":" + port;
                    }
                }
            }
            return endpoint;
        } catch (Exception e) {
            logger.error("Error extracting gRPC address from endpoint: {}", endpoint, e);
            return endpoint;
        }
    }

    public void shutdown() {
        channels.values().forEach(ManagedChannel::shutdown);
    }
}
