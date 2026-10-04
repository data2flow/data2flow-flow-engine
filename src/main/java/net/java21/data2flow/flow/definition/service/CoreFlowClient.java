package net.java21.data2flow.flow.definition.service;

import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.common.TransientFailures.CoreUnavailableException;
import net.java21.data2flow.flow.definition.domain.FlowRuntime;
import net.java21.data2flow.flow.definition.domain.RuntimeSnapshot;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.Overlay;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * core-api 내부 API 클라이언트(ADR-021: 토큰 없음, {@code X-CALLER-SERVICE: data2flow-flow-engine}, HTTP 80). 응답은 공통 봉투
 * {@code {header, response}}(목록은 {@code responses})이고 모르는 필드는 무시한다. ID는 문자열·숫자 둘 다 받는다(api-rules: ID는 문자열).
 *
 * <p>호출하는 API(정본 design/api/FLW-api.md §8, DEV-api.md §12): API-FLW-80 {@code GET /internal/core/flows/runtime?sinceVersion=},
 * API-FLW-81 {@code GET /internal/core/flows/{flow-id}/runtime}, API-DEV-128 {@code GET /internal/core/spaces/{space-id}/devices}.
 */
public class CoreFlowClient implements CoreFlowDirectory {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(CoreFlowClient.class);

    public static final String CALLER_HEADER = "X-CALLER-SERVICE";
    public static final String CALLER = "data2flow-flow-engine";

    private final HttpClient http;
    private final String baseUrl;
    private final FlowEngineProperties.Core settings;

    public CoreFlowClient(FlowEngineProperties.Core settings) {
        this.settings = settings;
        this.baseUrl = settings.baseUrl().endsWith("/") ? settings.baseUrl().substring(0, settings.baseUrl().length() - 1)
                : settings.baseUrl();
        this.http = HttpClient.newBuilder().connectTimeout(settings.connectTimeout()).build();
    }

    @Override
    public Optional<RuntimeSnapshot> runtime(Long sinceVersion) {
        Response r = get("/internal/core/flows/runtime" + (sinceVersion == null ? "" : "?sinceVersion=" + sinceVersion));
        if (r.status == 204) {
            return Optional.empty();
        }
        if (r.status == 404) {
            // 경로가 아직 없음(core 배포 전·순서 차이). 빈 목록으로 보면 적재한 플로우를 모두 내리고 타이머를 취소하므로 "변경 없음"으로 본다
            log.warn("core-api에 API-FLW-80이 없습니다(404). 적재한 플로우를 그대로 둡니다");
            return Optional.empty();
        }
        JsonNode body = r.response();
        List<FlowRuntime> flows = new ArrayList<>();
        JsonNode items = body.path("flows");
        if (items.isArray()) {
            for (JsonNode item : items.values()) {
                flows.add(parseFlow(item));
            }
        }
        Double org = Jsons.number(body, "organizationId");
        return Optional.of(new RuntimeSnapshot(body.path("version").asLong(0), org == null ? null : org.longValue(),
                List.copyOf(flows)));
    }

    @Override
    public Optional<FlowRuntime> flow(UUID flowId) {
        Response r = get("/internal/core/flows/" + flowId + "/runtime");
        if (r.status == 404) {
            return Optional.empty();
        }
        return Optional.of(parseFlow(r.response()));
    }

    @Override
    public Set<Long> measuringDevices(long organizationId, long spaceId, boolean includeDescendants) {
        Response r = get("/internal/core/spaces/" + spaceId + "/devices?relation=measures&includeDescendants=" + includeDescendants);
        if (r.status == 404) {
            return Set.of();
        }
        JsonNode list = r.json.has("responses") ? r.json.get("responses") : r.json.path("response");
        if (list.isObject() && list.has("devices")) {
            list = list.get("devices");
        }
        Set<Long> ids = new HashSet<>();
        if (list.isArray()) {
            for (JsonNode d : list.values()) {
                Double id = Jsons.number(d, "deviceId");
                if (id != null) {
                    ids.add(id.longValue());
                }
            }
        }
        return Set.copyOf(ids);
    }

    @Override
    public Set<Long> spaceDevices(long organizationId, long spaceId, boolean includeDescendants) {
        Response r = get("/internal/core/spaces/" + spaceId + "/devices?includeDescendants=" + includeDescendants);
        if (r.status == 404) {
            return Set.of();
        }
        Set<Long> ids = new HashSet<>();
        for (JsonNode d : list(r).values()) {
            Double id = Jsons.number(d, "deviceId");
            if (id != null) {
                ids.add(id.longValue());
            }
        }
        return Set.copyOf(ids);
    }

    @Override
    public List<JsonNode> activeEmergencyStops() {
        Response r = get("/internal/core/emergency-stops?active=true");
        return r.status == 404 ? List.of() : List.copyOf(list(r).values());
    }

    @Override
    public List<JsonNode> activeMaintenance() {
        Response r = get("/internal/core/maintenance-windows?status=ACTIVE");
        return r.status == 404 ? List.of() : List.copyOf(list(r).values());
    }

    @Override
    public List<JsonNode> scriptBundle(long organizationId) {
        Response r = get("/internal/core/scripts/runtime-bundle?organizationId=" + organizationId);
        if (r.status == 404 || r.status == 204) {
            return List.of();
        }
        return List.copyOf(r.response().path("scripts").values());
    }

    @Override
    public List<String> deviceTags(long organizationId, long deviceId) {
        Response r = get("/internal/core/devices/" + deviceId + "/runtime");
        if (r.status == 404) {
            return List.of();
        }
        List<String> tags = new ArrayList<>();
        r.response().path("tags").values().forEach(t -> tags.add(t.asString()));
        return List.copyOf(tags);
    }

    @Override
    public TelemetryPage telemetryHistory(long organizationId, List<Long> deviceIds, Instant from, Instant to, String cursor,
                                          int size) {
        StringBuilder q = new StringBuilder("/internal/core/telemetry/history?organizationId=").append(organizationId)
                .append("&from=").append(from).append("&to=").append(to).append("&size=").append(size);
        if (deviceIds != null && !deviceIds.isEmpty()) {
            q.append("&deviceIds=").append(deviceIds.stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(",")));
        }
        if (cursor != null) {
            q.append("&cursor=").append(java.net.URLEncoder.encode(cursor, java.nio.charset.StandardCharsets.UTF_8));
        }
        Response r = get(q.toString());
        if (r.status == 404) {
            throw new IllegalStateException("core-api에 과거 텔레메트리 조회(/internal/core/telemetry/history)가 없습니다");
        }
        List<CanonicalTelemetry> items = new ArrayList<>();
        for (JsonNode item : list(r).values()) {
            items.add(Jsons.MAPPER.treeToValue(item, CanonicalTelemetry.class));
        }
        JsonNode next = r.json.has("nextCursor") ? r.json.get("nextCursor") : r.response().path("nextCursor");
        Double total = Jsons.number(r.json, "totalCount");
        return new TelemetryPage(List.copyOf(items), next == null || next.isNull() || next.isMissingNode() ? null : next.asString(),
                total == null ? null : total.longValue());
    }

    /** 목록 응답(responses 또는 response 배열) */
    private static JsonNode list(Response r) {
        JsonNode list = r.json.has("responses") ? r.json.get("responses") : r.json.path("response");
        if (list.isObject() && list.has("devices")) {
            list = list.get("devices");
        }
        return list.isArray() ? list : Jsons.MAPPER.createArrayNode();
    }

    static FlowRuntime parseFlow(JsonNode item) {
        UUID flowId = UUID.fromString(item.path("flowId").asString());
        Double org = Jsons.number(item, "organizationId");
        Double version = Jsons.number(item, "activeVersion");
        JsonNode def = item.get("definition");
        FlowDefinition definition = def == null || def.isNull() ? null : Jsons.MAPPER.treeToValue(def, FlowDefinition.class);
        JsonNode o = item.path("overlay");
        Set<String> bypass = new HashSet<>();
        Set<String> debug = new HashSet<>();
        o.path("bypass").values().forEach(v -> bypass.add(v.asString()));
        o.path("debug").values().forEach(v -> debug.add(v.asString()));
        Overlay overlay = new Overlay(bypass, debug, o.path("revision").asLong(0));
        return new FlowRuntime(flowId, org == null ? 0 : org.longValue(), Jsons.text(item, "name"),
                Optional.ofNullable(Jsons.text(item, "kind")).orElse("FLOW"),
                Optional.ofNullable(Jsons.text(item, "status")).orElse("ACTIVE"),
                version == null ? 0 : version.intValue(), definition, overlay,
                Optional.ofNullable(Jsons.number(item, "rateLimitPerSec")).map(Double::intValue).orElse(0),
                Optional.ofNullable(Jsons.text(item, "pauseMode")).orElse("DROP"));
    }

    private Response get(String path) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(settings.readTimeout())
                .header(CALLER_HEADER, CALLER)
                .header("Accept", "application/json")
                .GET().build();
        try {
            HttpResponse<String> res = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() >= 500) {
                throw new CoreUnavailableException("core-api " + path + " → " + res.statusCode(), null);
            }
            if (res.statusCode() == 204 || res.statusCode() == 404) {
                return new Response(res.statusCode(), Jsons.object());
            }
            if (res.statusCode() >= 400) {
                throw new IllegalStateException("core-api " + path + " → " + res.statusCode() + " " + res.body());
            }
            return new Response(res.statusCode(), Jsons.MAPPER.readTree(res.body()));
        } catch (IOException e) {
            throw new CoreUnavailableException("core-api " + path + " 호출 실패: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CoreUnavailableException("core-api 호출이 중단되었습니다", e);
        }
    }

    private record Response(int status, JsonNode json) {
        JsonNode response() {
            return json.path("response");
        }
    }
}
