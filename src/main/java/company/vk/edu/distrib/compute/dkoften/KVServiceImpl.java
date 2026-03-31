package company.vk.edu.distrib.compute.dkoften;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class KVServiceImpl implements KVService {
    private final HttpServer server;
    private final DaoImpl dao = new DaoImpl("storage.db");
    private final Logger logger = LoggerFactory.getLogger("service");
    private final ExecutorService executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
    );

    private KVServiceImpl(int port) {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(executor);
            server.createContext("/v0/entity", exchange -> {
                try (exchange) {
                    try {
                        handleRequest(exchange);
                    } catch (IllegalArgumentException e) {
                        exchange.sendResponseHeaders(400, 0);
                    } catch (NoSuchElementException e) {
                        exchange.sendResponseHeaders(404, 0);
                    } catch (Exception e) {
                        exchange.sendResponseHeaders(500, 0);
                        if (logger.isErrorEnabled()) {
                            logger.error("Error processing request", e);
                        }
                    }
                }
            });
            server.createContext("/v0/status", exchange -> {
                try (exchange) {
                    exchange.sendResponseHeaders(200, 0);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleRequest(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();

        if (logger.isDebugEnabled()) {
            logger.debug("Received {} request for {}", method, exchange.getRequestURI());
        }

        String query = exchange.getRequestURI().getQuery();
        if (query == null || !query.startsWith("id=")) {
            exchange.sendResponseHeaders(400, 0);
            return;
        }

        String key = query.substring(3);
        if (key.isEmpty()) {
            exchange.sendResponseHeaders(400, 0);
            return;
        }

        switch (method) {
            case "GET":
                byte[] value = dao.get(key);
                exchange.sendResponseHeaders(200, value.length);
                if (logger.isDebugEnabled()) {
                    logger.debug("Returning value of length {}", value.length);
                }
                exchange.getResponseBody().write(value);
                break;
            case "PUT":
                byte[] newValue = exchange.getRequestBody().readAllBytes();
                if (logger.isDebugEnabled()) {
                    logger.debug("Upserting key {} with value of length {}", key, newValue.length);
                }
                dao.upsert(key, newValue);
                exchange.sendResponseHeaders(201, 0);
                break;
            case "DELETE":
                dao.delete(key);
                exchange.sendResponseHeaders(202, 0);
                break;
            default:
                exchange.sendResponseHeaders(405, 0);
        }
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        executor.shutdown();
        try {
            dao.close();
        } catch (IOException e) {
            if (logger.isErrorEnabled()) {
                logger.error("Error closing dao", e);
            }
        }
    }

    public static class Factory extends KVServiceFactory {
        @Override
        protected KVService doCreate(int port) throws IOException {
            return new KVServiceImpl(port);
        }
    }
}
