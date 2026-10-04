package net.java21.data2flow.flow.runtime;

import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.FlowDebugMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.IntegrationTestSupport;
import net.java21.data2flow.flow.support.TestInfrastructure;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 라이브 뷰·추적·지표(FLW-03.01·03.04·05.05)와 실행 모드·일시 정지(FLW-05.07·08.04) 통합 시험(Testcontainers PostgreSQL 18 + RabbitMQ 3.13).
 */
class LiveViewModesIT extends IntegrationTestSupport {

    private static final MessageCodec CODEC = MessageCodec.create();
    private static final Clock WALL = Clock.systemUTC();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Autowired
    FlowRegistry registry;
    @Autowired
    JdbcClient jdbc;
    @LocalServerPort
    int port;

    private void deploy(UUID flow, int version, String status, FlowDefinition definition, List<String> debug, String pauseMode) {
        TestInfrastructure.CORE.put(flow, FlowFixtures.ORG, version, status, definition, List.of(), debug, 1, 1000, pauseMode);
        await().atMost(Duration.ofSeconds(20)).until(() -> registry.get(flow).map(f -> f.version() == version
                && f.status().equals(status)).orElse(false));
    }

    private static CanonicalTelemetry reading(long device, double value) {
        Instant now = Instant.now(WALL);
        return CanonicalTelemetry.builder().organizationId(FlowFixtures.ORG).sourceId(3).externalId("lv-" + device).deviceId(device)
                .modelId("lv").measuredAt(now).receivedAt(now).metric(CanonicalTelemetry.Metric.of("temperature", value, "℃"))
                .rawMessageId(1).build();
    }

    private static FlowDefinition thresholdFlow(long device) {
        return FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["%d"]}}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"temperature","op":">","value":27}},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"deviceId":"%d"},"capability":"Switch","command":"set","args":{"on":true}}}],
                 "wires":[{"from":"n-trg00001","to":"n-thr00001"},{"from":"n-thr00001","port":"true","to":"n-act00001"}]}"""
                .formatted(device, device + 1));
    }

    private JsonNode get(String path) throws Exception {
        HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        JsonNode body = net.java21.data2flow.flow.plan.domain.Jsons.MAPPER.readTree(r.body());
        ((tools.jackson.databind.node.ObjectNode) body).put("_status", r.statusCode());
        return body;
    }

    @Test
    @DisplayName("[FLW-03.01][FLW-03.04][FLW-05.05][AT-FLW-04.1] TC-FLW-060·066·071 node.stats가 2초 안에 data2flow.debug로, 디버그 노드 샘플 ≤50/초, 추적 API·지표 API")
    void liveViewTraceAndMetrics() throws Exception {
        UUID flow = UUID.randomUUID();
        String queue = "it.debug." + flow;
        STREAMS.channel().queueDeclare(queue, false, false, true, MessagingNames.debugQueueArguments());
        STREAMS.channel().queueBind(queue, MessagingNames.EXCHANGE_DEBUG, FlowDebugMessage.routingKey(flow.toString()));
        deploy(flow, 2, "ACTIVE", thresholdFlow(7401), List.of("n-thr00001"), "DROP");

        CanonicalTelemetry first = reading(7401, 28);
        Instant sent = Instant.now(WALL);
        STREAMS.publish(first);
        List<FlowDebugMessage> received = new ArrayList<>();
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            STREAMS.drain(queue).forEach(b -> received.add(CODEC.read(b, FlowDebugMessage.class)));
            return received.stream().anyMatch(m -> m.type() == FlowDebugMessage.Type.NODE_STATS);
        });
        FlowDebugMessage stats = received.stream().filter(m -> m.type() == FlowDebugMessage.Type.NODE_STATS).findFirst().orElseThrow();
        assertThat(Duration.between(sent, stats.t())).as("표시 지연 2초 이내").isLessThanOrEqualTo(Duration.ofSeconds(2));
        assertThat(stats.version()).isEqualTo(2);
        assertThat(stats.stats()).anySatisfy(s -> {
            assertThat(s.nodeId()).isEqualTo("n-thr00001");
            assertThat(s.out()).containsEntry("true", 1L);
            assertThat(s.status()).isEqualTo(FlowDebugMessage.NodeStatus.OK);
        });
        assertThat(received).anySatisfy(m -> {
            assertThat(m.type()).isEqualTo(FlowDebugMessage.Type.NODE_SAMPLE);
            assertThat(m.sample().nodeId()).isEqualTo("n-thr00001");
        });

        // 추적: 디버그 노드를 지난 메시지는 1시간 보관(API-FLW-41)
        await().atMost(Duration.ofSeconds(10)).until(() -> get("/internal/flow/traces/" + first.messageId() + "?flowId=" + flow)
                .path("_status").asInt() == 200);
        JsonNode trace = get("/internal/flow/traces/" + first.messageId() + "?flowId=" + flow).path("response");
        assertThat(trace.path("version").asInt()).isEqualTo(2);
        assertThat(trace.path("steps").size()).isEqualTo(3);
        assertThat(trace.path("steps").get(2).path("action").path("kind").asString()).isEqualTo("COMMAND");
        assertThat(get("/internal/flow/traces/" + UUID.randomUUID()).path("_status").asInt()).isEqualTo(404);

        // 디버그 노드 샘플 상한: 같은 초에 많이 들어와도 초당 50건 이하(BR-FLW-12)
        for (int i = 0; i < 120; i++) {
            STREAMS.publishAsync(reading(7401, 28 + i % 3));
        }
        await().atMost(Duration.ofSeconds(30)).until(() -> STREAMS.commands(flow).size() >= 121);
        STREAMS.drain(queue).forEach(b -> received.add(CODEC.read(b, FlowDebugMessage.class)));
        Map<String, Integer> perSecond = new HashMap<>();
        received.stream().filter(m -> m.type() == FlowDebugMessage.Type.NODE_SAMPLE && m.sample().nodeId().equals("n-thr00001"))
                .forEach(m -> perSecond.merge(m.t().getEpochSecond() + ":" + m.sample().direction(), 1, Integer::sum));
        assertThat(perSecond.values()).allSatisfy(n -> assertThat(n).isLessThanOrEqualTo(50));

        // 지표(API-FLW-14 내부): 5초마다 쓰는 분 단위 지표
        await().atMost(Duration.ofSeconds(20)).until(() -> get("/internal/flow/flows/" + flow + "/metrics?window=1h")
                .path("response").path("summary").path("executions").asLong() >= 121);
        JsonNode metrics = get("/internal/flow/flows/" + flow + "/metrics?window=1h").path("response");
        assertThat(metrics.path("summary").path("actions").path("command").asLong()).isGreaterThanOrEqualTo(121);
        assertThat(metrics.path("nodes").findValuesAsString("nodeId")).contains("n-thr00001");
        assertThat(get("/internal/flow/flows/" + flow + "/metrics?window=3h").path("_status").asInt()).isEqualTo(400);
    }

    private static FlowDefinition delayFlow(long device, String mode, int max) {
        return FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","mode":{"concurrency":"%s","keyBy":"deviceId","max":%d},"nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["%d"]}}},
                  {"id":"n-dly00001","type":"flow.delay","config":{"duration":"PT3S"}},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"deviceId":"%d"},"capability":"Switch","command":"set","args":{"on":true}}}],
                 "wires":[{"from":"n-trg00001","to":"n-dly00001"},{"from":"n-dly00001","to":"n-act00001"}]}"""
                .formatted(mode, max, device, device + 1));
    }

    private List<String> timerStatuses(UUID flow) {
        return jdbc.sql("SELECT status FROM data2flow_flow.flow_timers WHERE flow_id = :f ORDER BY id").param("f", flow)
                .query(String.class).list();
    }

    @Test
    @DisplayName("[FLW-05.07][AT-FLW-15.1] TC-FLW-121 single: 대기 중인 실행이 있으면 같은 기기 새 트리거는 건너뜀(버린 트리거 지표)")
    void singleMode() {
        UUID flow = UUID.randomUUID();
        deploy(flow, 1, "ACTIVE", delayFlow(7501, "single", 10), List.of(), "DROP");
        STREAMS.publish(reading(7501, 28));
        await().atMost(Duration.ofSeconds(10)).until(() -> timerStatuses(flow).size() == 1);
        STREAMS.publish(reading(7501, 29));
        await().atMost(Duration.ofSeconds(20)).until(() -> STREAMS.commands(flow).size() == 1);
        await().during(Duration.ofSeconds(4)).atMost(Duration.ofSeconds(10)).until(() -> STREAMS.commands(flow).size() == 1);
        assertThat(timerStatuses(flow)).containsExactly("FIRED");
        await().atMost(Duration.ofSeconds(10)).until(() -> jdbc.sql(
                "SELECT coalesce(sum(dropped),0) FROM data2flow_flow.flow_metric_minutes WHERE flow_id = :f AND node_id = '*'")
                .param("f", flow).query(Long.class).single() == 1);
    }

    @Test
    @DisplayName("[FLW-05.07][AT-FLW-15.2] TC-FLW-122 restart: 같은 기기 새 트리거가 오면 진행 중 실행의 대기 타이머를 취소하고 새로 시작")
    void restartMode() {
        UUID flow = UUID.randomUUID();
        deploy(flow, 1, "ACTIVE", delayFlow(7601, "restart", 10), List.of(), "DROP");
        STREAMS.publish(reading(7601, 28));
        await().atMost(Duration.ofSeconds(10)).until(() -> timerStatuses(flow).size() == 1);
        STREAMS.publish(reading(7601, 29));
        await().atMost(Duration.ofSeconds(10)).until(() -> timerStatuses(flow).equals(List.of("CANCELLED", "WAITING"))
                || timerStatuses(flow).equals(List.of("CANCELLED", "FIRED")));
        await().atMost(Duration.ofSeconds(20)).until(() -> STREAMS.commands(flow).size() == 1);
    }

    @Test
    @DisplayName("[FLW-05.07][AT-FLW-15.3] TC-FLW-123 parallel max 10: 동시 트리거 12건 → 10건 실행(대기 타이머), 2건 대기 후 실행")
    void parallelMode() {
        UUID flow = UUID.randomUUID();
        deploy(flow, 1, "ACTIVE", delayFlow(7701, "parallel", 10), List.of(), "DROP");
        for (int i = 0; i < 12; i++) {
            STREAMS.publish(reading(7701, 28));
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> jdbc.sql(
                "SELECT count(*) FROM data2flow_flow.flow_paused_triggers WHERE flow_id = :f AND reason = 'QUEUED'").param("f", flow)
                .query(Long.class).single() == 2 || STREAMS.commands(flow).size() >= 10);
        assertThat(timerStatuses(flow).stream().filter(s -> !s.equals("CANCELLED")).count()).isLessThanOrEqualTo(12);
        await().atMost(Duration.ofSeconds(30)).until(() -> STREAMS.commands(flow).size() == 12);
        assertThat(jdbc.sql("SELECT count(*) FROM data2flow_flow.flow_paused_triggers WHERE flow_id = :f").param("f", flow)
                .query(Long.class).single()).isZero();
        assertThat(timerStatuses(flow)).hasSize(12).allMatch("FIRED"::equals);
    }

    @Test
    @DisplayName("[FLW-08.04][AT-FLW-10.1·10.2] TC-FLW-194·195 일시 정지 DROP은 버리고 세며(재개 뒤에도 처리 안 함), BUFFER는 보관했다가 재개 뒤 순서대로 처리")
    void pauseDropAndBuffer() {
        UUID dropped = UUID.randomUUID();
        deploy(dropped, 1, "PAUSED", thresholdFlow(7801), List.of(), "DROP");
        for (int i = 0; i < 10; i++) {
            STREAMS.publish(reading(7801, 28));
        }
        await().atMost(Duration.ofSeconds(20)).until(() -> jdbc.sql(
                "SELECT coalesce(sum(dropped),0) FROM data2flow_flow.flow_metric_minutes WHERE flow_id = :f AND node_id = '*'")
                .param("f", dropped).query(Long.class).single() == 10);
        deploy(dropped, 1, "ACTIVE", thresholdFlow(7801), List.of(), "DROP");
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> STREAMS.commands(dropped).isEmpty());

        UUID buffered = UUID.randomUUID();
        deploy(buffered, 1, "PAUSED", thresholdFlow(7901), List.of(), "BUFFER");
        List<String> order = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            CanonicalTelemetry t = reading(7901, 28);
            order.add(t.messageId().toString());
            STREAMS.publish(t);
        }
        await().atMost(Duration.ofSeconds(20)).until(() -> jdbc.sql(
                "SELECT count(*) FROM data2flow_flow.flow_paused_triggers WHERE flow_id = :f AND reason = 'PAUSED'").param("f", buffered)
                .query(Long.class).single() == 5);
        assertThat(STREAMS.commands(buffered)).isEmpty();
        deploy(buffered, 1, "ACTIVE", thresholdFlow(7901), List.of(), "BUFFER");
        await().atMost(Duration.ofSeconds(20)).until(() -> STREAMS.commands(buffered).size() == 5);
        assertThat(STREAMS.commands(buffered).stream().map(c -> c.source().triggerMessageId()).toList())
                .as("받은 순서대로").containsExactlyElementsOf(order);
    }
}
