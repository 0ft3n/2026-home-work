package company.vk.edu.distrib.compute.dkoften.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

public class GrpcServerManager {
    private final Server server;
    private final Logger logger = LoggerFactory.getLogger("grpc");

    public GrpcServerManager(int grpcPort, GrpcKVServiceImpl kvService) {
        try {
            this.server = ServerBuilder.forPort(grpcPort)
                    .addService(kvService)
                    .build();
        } catch (Exception e) {
            throw new UncheckedIOException(new IOException("Failed to create gRPC server", e));
        }
    }

    public void start() {
        try {
            if (logger.isInfoEnabled()) {
                logger.info("Starting gRPC server...");
            }
            server.start();
            if (logger.isInfoEnabled()) {
                logger.info("gRPC server started successfully");
            }
        } catch (IOException e) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to start gRPC server", e);
            }
            throw new UncheckedIOException(e);
        }
    }

    public void stop() {
        try {
            server.shutdown();
            if (logger.isInfoEnabled()) {
                logger.info("gRPC server stopped");
            }
        } catch (Exception e) {
            logger.error("Error stopping gRPC server", e);
        }
    }
}
