package net.java21.data2flow.flow.support;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.flow.plan.domain.Jsons;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * core-api 내부 API 대역(JDK HttpServer, 별도 JVM 엔진도 접속): API-FLW-80·81, API-DEV-128을 FLW-api §8 모양으로 돌려준다.
 * 시험이 {@link #put}으로 플로우(정의·상태·오버레이)를 바꾸면 목록 버전이 오른다.
 */
public final class CoreApiStub implements AutoCloseable {

    private final HttpServer server;
    private final Map<UUID, ObjectNode> flows = new ConcurrentHashMap<>();
    private final Map<Long, Set<Long>> spaces = new ConcurrentHashMap<>();
    private final AtomicLong version = new AtomicLong(1);
    private final List<String> calls = new CopyOnWriteArrayList<>();
    private volatile long organizationId = FlowFixtures.ORG;
    private volatile boolean failing;

    public CoreApiStub() {
        try {
            server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        server.createContext("/internal/core/", this::handle);
        server.start();
    }

    public String baseUrl() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    public CoreApiStub organization(long id) {
        organizationId = id;
        return this;
    }

    public void failing(boolean value) {
        failing = value;
    }

    public List<String> calls() {
        return List.copyOf(calls);
    }

    /** 플로우를 등록·변경한다(상태 ACTIVE·PAUSED·DISABLED, 오버레이 리비전 0) */
    public void put(UUID flowId, long organization, int activeVersion, String status, FlowDefinition definition) {
        put(flowId, organization, activeVersion, status, definition, List.of(), 0);
    }

    public void put(UUID flowId, long organization, int activeVersion, String status, FlowDefinition definition,
                    List<String> bypass, long overlayRevision) {
        ObjectNode item = Jsons.object();
        item.put("flowId", flowId.toString());
        item.put("organizationId", Long.toString(organization));
        item.put("name", "flow-" + flowId.toString().substring(0, 8));
        item.put("kind", "FLOW");
        item.put("status", status);
        item.put("activeVersion", activeVersion);
        item.put("rateLimitPerSec", 100);
        item.put("pauseMode", "DROP");
        item.set("definition", Jsons.MAPPER.valueToTree(definition));
        ObjectNode overlay = item.putObject("overlay");
        ArrayNode b = overlay.putArray("bypass");
        bypass.forEach(b::add);
        overlay.putArray("debug");
        overlay.put("revision", overlayRevision);
        flows.put(flowId, item);
        version.incrementAndGet();
    }

    public void remove(UUID flowId) {
        flows.remove(flowId);
        version.incrementAndGet();
    }

    public void space(long spaceId, Set<Long> devices) {
        spaces.put(spaceId, devices);
    }

    private void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String query = exchange.getRequestURI().getQuery();
        calls.add(path + (query == null ? "" : "?" + query) + " " + exchange.getRequestHeaders().getFirst("X-CALLER-SERVICE"));
        if (failing) {
            send(exchange, 503, "{}");
            return;
        }
        if (path.equals("/internal/core/flows/runtime")) {
            if (query != null && query.contains("sinceVersion=" + version.get())) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            ObjectNode response = Jsons.object();
            response.put("version", version.get());
            response.put("organizationId", Long.toString(organizationId));
            ArrayNode list = response.putArray("flows");
            flows.values().stream().filter(f -> !"DISABLED".equals(f.path("status").asString())).forEach(list::add);
            send(exchange, 200, envelope(response));
            return;
        }
        if (path.startsWith("/internal/core/flows/") && path.endsWith("/runtime")) {
            String id = path.substring("/internal/core/flows/".length(), path.length() - "/runtime".length());
            ObjectNode flow = flows.get(UUID.fromString(id));
            if (flow == null) {
                send(exchange, 404, "{\"header\":{\"isSuccessful\":false,\"resultCode\":\"RESOURCE_NOT_FOUND\"}}");
            } else {
                send(exchange, 200, envelope(flow));
            }
            return;
        }
        if (path.startsWith("/internal/core/spaces/") && path.endsWith("/devices")) {
            long spaceId = Long.parseLong(path.split("/")[4]);
            ObjectNode body = Jsons.object();
            body.putObject("header").put("isSuccessful", true).put("resultCode", "SUCCESS");
            ArrayNode list = body.putArray("responses");
            spaces.getOrDefault(spaceId, Set.of()).forEach(d -> list.addObject().put("deviceId", Long.toString(d))
                    .put("relation", "MEASURES"));
            body.put("totalCount", list.size());
            send(exchange, 200, body.toString());
            return;
        }
        send(exchange, 404, "{}");
    }

    private static String envelope(ObjectNode response) {
        ObjectNode body = Jsons.object();
        body.putObject("header").put("isSuccessful", true).put("resultCode", "SUCCESS").put("resultMessage", "성공");
        body.set("response", response);
        return body.toString();
    }

    private static void send(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
