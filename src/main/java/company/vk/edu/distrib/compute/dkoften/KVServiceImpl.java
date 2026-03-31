package company.vk.edu.distrib.compute.dkoften;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public final class KVServiceImpl implements KVService {
    private final HttpServer server;
    private final DaoImpl dao;
    private final Logger logger = LoggerFactory.getLogger("service");

    KVServiceImpl(int port) {
        dao = new DaoImpl("storage.db");
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/v0/entity", this::handleEntity);
            server.createContext("/v0/status", this::handleStatus);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void handleStatus(HttpExchange exchange) {
        try (exchange) {
            exchange.sendResponseHeaders(200, 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void handleEntity(HttpExchange exchange) {
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
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
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

        handleMethod(exchange, method, key);
    }

    private void handleMethod(HttpExchange exchange, String method, String key) throws IOException {
        switch (method) {
            case "GET":
                handleGet(exchange, key);
                break;
            case "PUT":
                handlePut(exchange, key);
                break;
            case "DELETE":
                dao.delete(key);
                exchange.sendResponseHeaders(202, 0);
                break;
            default:
                exchange.sendResponseHeaders(405, 0);
                break;
        }
    }

    private void handleGet(HttpExchange exchange, String key) throws IOException {
        byte[] value = dao.get(key);
        exchange.sendResponseHeaders(200, value.length);
        if (logger.isDebugEnabled()) {
            logger.debug("Returning value of length {}", value.length);
        }
        exchange.getResponseBody().write(value);
    }

    private void handlePut(HttpExchange exchange, String key) throws IOException {
        byte[] newValue = exchange.getRequestBody().readAllBytes();
        if (logger.isDebugEnabled()) {
            logger.debug("Upserting key {} with value of length {}", key, newValue.length);
        }
        dao.upsert(key, newValue);
        exchange.sendResponseHeaders(201, 0);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
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
