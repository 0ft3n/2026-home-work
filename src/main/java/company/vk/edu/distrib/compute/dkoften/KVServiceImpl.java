package company.vk.edu.distrib.compute.dkoften;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.dkoften.sharding.KVClusterImpl;
import company.vk.edu.distrib.compute.dkoften.sharding.ShardingBalancer;
import company.vk.edu.distrib.compute.dkoften.storage.DaoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.NoSuchElementException;

public final class KVServiceImpl implements KVService {
    private final HttpServer server;
    private final HttpClient client;
    @Nullable
    private final ShardingBalancer balancer;
    private final DaoImpl dao;
    private final Logger logger = LoggerFactory.getLogger("service");
    @Nullable
    private final KVClusterImpl cluster;
    private static final String IS_PROXY_HEADER = "X-Cluster-Proxied";

    KVServiceImpl(int port) {
        this(
                port,
                HttpClient.newHttpClient(),
                null
        );
    }

    KVServiceImpl(
            int port,
            HttpClient client,
            @Nullable KVClusterImpl cluster
    ) {
        this.cluster = cluster;
        if (cluster != null) {
            this.balancer = new ShardingBalancer();
        } else {
            this.balancer = null;
        }
        dao = new DaoImpl(System.getProperty("user.home") + java.io.File.separator + "storage-" + port + ".db");
        this.client = client;
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

    private void handleRequest(HttpExchange exchange) throws IOException, InterruptedException {
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

        boolean shouldProxy = cluster != null && !isInternalExchange(exchange) && balancer != null;

        if (shouldProxy) {
            handleProxyMethod(exchange, method, key);
        } else {
            handleMethod(exchange, method, key);
        }
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

    private void handleProxyMethod(HttpExchange exchange, String method, String key) throws IOException,
            InterruptedException {
        assert balancer != null;
        assert cluster != null;
        String endpoint = balancer.selectFor(cluster.getEndpoints(), key);

        switch (method) {
            case "GET":
                proxyGet(exchange, key, endpoint);
                break;
            case "PUT":
                proxyPut(exchange, key, endpoint);
                break;
            case "DELETE":
                proxyDelete(exchange, key, endpoint);
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

    private void proxyGet(HttpExchange exchange, String key, String endpoint) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder()
                .uri(java.net.URI.create(endpoint + "/v0/entity?id=" + key))
                .header(IS_PROXY_HEADER, "true")
                .GET()
                .build();

        var response = this.client.send(request, java.net.http.HttpResponse.BodyHandlers.ofByteArray());
        exchange.sendResponseHeaders(response.statusCode(), response.body().length);
        if (logger.isDebugEnabled()) {
            logger.debug("Proxied GET request for key {} to endpoint {}, received status {}, value length {}", key,
                    endpoint, response.statusCode(), response.body().length);
        }
        exchange.getResponseBody().write(response.body());
    }

    private void proxyPut(HttpExchange exchange, String key, String endpoint) throws IOException, InterruptedException {
        byte[] newValue = exchange.getRequestBody().readAllBytes();
        var request = HttpRequest.newBuilder()
                .uri(java.net.URI.create(endpoint + "/v0/entity?id=" + key))
                .header(IS_PROXY_HEADER, "true")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(newValue))
                .build();

        var response = this.client.send(request, java.net.http.HttpResponse.BodyHandlers.discarding());
        exchange.sendResponseHeaders(response.statusCode(), 0);
        if (logger.isDebugEnabled()) {
            logger.debug("Proxied PUT request for key {} to endpoint {}, received status {}", key, endpoint,
                    response.statusCode());
        }
    }

    private void proxyDelete(HttpExchange exchange, String key, String endpoint) throws IOException,
            InterruptedException {
        var request = HttpRequest.newBuilder()
                .uri(java.net.URI.create(endpoint + "/v0/entity?id=" + key))
                .header(IS_PROXY_HEADER, "true")
                .DELETE()
                .build();

        var response = this.client.send(request, java.net.http.HttpResponse.BodyHandlers.discarding());
        exchange.sendResponseHeaders(response.statusCode(), 0);
        if (logger.isDebugEnabled()) {
            logger.debug("Proxied DELETE request for key {} to endpoint {}, received status {}", key, endpoint,
                    response.statusCode());
        }
    }

    private boolean isInternalExchange(HttpExchange exchange) {
        return exchange.getRequestHeaders().containsKey(IS_PROXY_HEADER);
    }

    @Override
    public void start() {
        try {
            server.start();
        } catch (IllegalStateException ignored) {
            if (logger.isDebugEnabled()) {
                logger.debug("Tried starting an already running service");
            }
        }
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

        public KVService create(int port, KVClusterImpl cluster) throws IOException {
            return new KVServiceImpl(port, HttpClient.newHttpClient(), cluster);
        }
    }
}
