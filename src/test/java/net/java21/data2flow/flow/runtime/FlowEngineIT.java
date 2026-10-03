package net.java21.data2flow.flow.runtime;

import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.FlowApplyReported;
import net.java21.data2flow.flow.definition.service.FlowSynchronizer;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.service.FlowRuntimeService;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.IntegrationTestSupport;
import net.java21.data2flow.flow.support.TestInfrastructure;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * flow-engine 서비스 전체 통합 시험(실제 스트림 소비·지속 타이머·아웃박스 릴레이·설정 수신, Testcontainers PostgreSQL 18 + RabbitMQ 3.13).
 * 플로우마다 다른 기기를 대상으로 해서 시험끼리 섞이지 않는다.
 */
class FlowEngineIT extends IntegrationTestSupport {

    private static final MessageCodec CODEC = MessageCodec.create();
    private static final Clock WALL = Clock.systemUTC();

    @Autowired
    FlowRegistry registry;
    @Autowired
    FlowSynchronizer synchronizer;
    @Autowired
    FlowRuntimeService runtime;
    @Autowired
    JdbcClient jdbc;
    @LocalServerPort
    int port;

    /** 기기 목록 트리거 → (집계) → 임계값 → 기기 제어 */
    private static FlowDefinition simple(long device, String threshold, boolean aggregate, long target) {
        return FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["%d"]},"metrics":["temperature"]}},
                  {"id":"n-agg00001","type":"transform.aggregate","config":{"window":"PT10M","fn":"avg","groupBy":"device"}},
                  {"id":"n-thr00001","type":"condition.threshold","config":%s},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"deviceId":"%d"},"capability":"Switch","command":"set","args":{"on":true}}}],
                 "wires":[{"from":"n-trg00001","to":"%s"},{"from":"n-agg00001","to":"n-thr00001"},{"from":"n-thr00001","port":"true","to":"n-act00001"}]}"""
                .formatted(device, threshold, target, aggregate ? "n-agg00001" : "n-thr00001"));
    }

    private void deploy(UUID flow, int version, FlowDefinition definition) {
        TestInfrastructure.CORE.put(flow, FlowFixtures.ORG, version, "ACTIVE", definition);
        await().atMost(Duration.ofSeconds(20)).until(() -> registry.get(flow).map(f -> f.version() == version).orElse(false));
    }

    private static CanonicalTelemetry temperature(long device, double value) {
        Instant now = Instant.now(WALL);
        return CanonicalTelemetry.builder().organizationId(FlowFixtures.ORG).sourceId(3).externalId("it-" + device).deviceId(device)
                .modelId("it").spaceId(900L + device).measuredAt(now).receivedAt(now)
                .metric(CanonicalTelemetry.Metric.of("temperature", value, "℃")).rawMessageId(1).build();
    }

    private long outboxCount(UUID flow) {
        return jdbc.sql("SELECT count(*) FROM data2flow_flow.flow_outboxes WHERE flow_id = :f").param("f", flow).query(Long.class).single();
    }

    private int aggregateCount(UUID flow, long device) {
        String state = jdbc.sql("""
                        SELECT state::text FROM data2flow_flow.flow_node_state WHERE flow_id = :f AND node_id = 'n-agg00001' AND target_key = :k""")
                .param("f", flow).param("k", "device:" + device).query(String.class).optional().orElse("{}");
        return net.java21.data2flow.flow.plan.domain.Jsons.MAPPER.readTree(state).path("samples").size();
    }

    @Test
    @DisplayName("[FLW-03.07][FLW-05.01][FLW-05.02][AT-FLW-24.2] 고온이면 냉방(공간 31): 평균 27℃ 초과 2초 지속 → 지속 타이머 → action.commands에 Thermostat.set 1건, 적용 보고(EVT-FLW-02·API-FLW-82)")
    void coolingClosedLoop() throws Exception {
        UUID flow = UUID.randomUUID();
        TestInfrastructure.CORE.space(FlowFixtures.SPACE, java.util.Set.of(1L, 2L));
        deploy(flow, 1, FlowFixtures.cooling(27, "PT2S", 24));
        CanonicalTelemetry first = FlowFixtures.temperature(1, 28.0, Instant.now(WALL));

        CanonicalTelemetry second = FlowFixtures.temperature(2, 27.6, Instant.now(WALL));
        STREAMS.publish(first);
        STREAMS.publish(second);

        await().atMost(Duration.ofSeconds(30)).until(() -> STREAMS.commands(flow).size() == 1);
        ActionRequest command = STREAMS.commands(flow).getFirst();
        // 두 기기는 다른 파티션일 수 있어 먼저 처리된 메시지가 지속 판정을 시작한다. 멱등 키의 원인 메시지는 그 메시지다(BR-FLW-13)
        assertThat(command.idempotencyKey()).isIn(
                ActionIdempotencyKeys.flow(flow.toString(), "n-act00001", first.messageId().toString()),
                ActionIdempotencyKeys.flow(flow.toString(), "n-act00001", second.messageId().toString()));
        assertThat(command.commandPayload().target().spaceId()).isEqualTo(FlowFixtures.SPACE);
        assertThat(command.commandPayload().args()).containsEntry("mode", "cool");
        assertThat(jdbc.sql("SELECT status FROM data2flow_flow.flow_timers WHERE flow_id = :f").param("f", flow).query(String.class).list())
                .containsExactly("FIRED");

        // 적용 보고: flow_instance_versions(원천) → EVT-FLW-02 → API-FLW-82
        List<FlowApplyReported> reports = new ArrayList<>();
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            for (byte[] body : STREAMS.drain("it.flow.events")) {
                DomainEvent<?> e = CODEC.readEvent(body);
                if (e.payload() instanceof FlowApplyReported r && r.flowId().equals(flow.toString())) {
                    reports.add(r);
                }
            }
            return !reports.isEmpty();
        });
        assertThat(reports.getFirst().appliedVersion()).isEqualTo(1);
        assertThat(reports.getFirst().instanceId()).isEqualTo("flow-engine-test");
        HttpResponse<String> status = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                URI.create("http://localhost:" + port + "/internal/flow/flows/" + flow + "/apply-status")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(status.statusCode()).isEqualTo(200);
        assertThat(status.body()).contains("\"appliedVersion\":1").contains("flow-engine-test");
    }

    @Test
    @DisplayName("[FLW-05.01][AT-FLW-24.1] TC-FLW-088 트리거→임계값→제어, 200건: 수신 시각부터 아웃박스 기록까지 p95 ≤ 500ms")
    void latency() {
        UUID flow = UUID.randomUUID();
        deploy(flow, 1, FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"latency"}}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"temperature","op":">","value":27}},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"deviceId":"77"},"capability":"Switch","command":"set","args":{"on":true}}}],
                 "wires":[{"from":"n-trg00001","to":"n-thr00001"},{"from":"n-thr00001","port":"true","to":"n-act00001"}]}"""));

        java.util.Map<String, Instant> received = new java.util.HashMap<>();
        for (int i = 0; i < 200; i++) {
            Instant now = Instant.now(WALL);
            CanonicalTelemetry t = CanonicalTelemetry.builder().organizationId(FlowFixtures.ORG).sourceId(3).externalId("lat-" + i)
                    .deviceId(5000 + (i % 20)).modelId("latency").measuredAt(now).receivedAt(now)
                    .metric(CanonicalTelemetry.Metric.of("temperature", 28, "℃")).rawMessageId(1).build();
            received.put(t.messageId().toString(), now);
            STREAMS.publish(t);
        }

        await().atMost(Duration.ofSeconds(60)).until(() -> outboxCount(flow) == 200);
        List<Long> latencies = jdbc.sql("SELECT trigger_message_id, created_at FROM data2flow_flow.flow_outboxes WHERE flow_id = :f")
                .param("f", flow).query((rs, n) -> Duration.between(received.get(rs.getString(1).trim()),
                        rs.getTimestamp(2).toInstant()).toMillis()).list().stream().sorted().toList();
        long p95 = latencies.get((int) Math.ceil(latencies.size() * 0.95) - 1);
        System.out.printf("[TC-FLW-088] 200건, 수신→아웃박스 기록 p50 %dms, p95 %dms, 최대 %dms%n", latencies.get(latencies.size() / 2), p95,
                latencies.getLast());
        assertThat(p95).isLessThanOrEqualTo(500);
        await().atMost(Duration.ofSeconds(30)).until(() -> STREAMS.commands(flow).size() == 200);
    }

    @Test
    @DisplayName("[FLW-05.01][FLW-05.02] TC-FLW-091 같은 오프셋을 다시 소비(되감기)해도 아웃박스 1행·노드 상태 갱신 1회·발행 1회, 다른 오프셋으로 다시 와도 멱등 키로 1행")
    void rewindIsIdempotent() {
        UUID flow = UUID.randomUUID();
        deploy(flow, 1, simple(6101, "{\"metric\":\"temperature\",\"op\":\">\",\"value\":27}", true, 6101));
        CanonicalTelemetry t = temperature(6101, 28);

        STREAMS.publish(t);
        await().atMost(Duration.ofSeconds(20)).until(() -> STREAMS.commands(flow).size() == 1);
        var progress = jdbc.sql("SELECT stream_partition, watermark_offset FROM data2flow_flow.flow_partition_progress WHERE flow_id = :f")
                .param("f", flow).query((rs, n) -> new long[]{rs.getInt(1), rs.getLong(2)}).single();

        assertThat(runtime.processTelemetry(t, (int) progress[0], progress[1], () -> false)).isTrue();          // 되감기
        assertThat(runtime.processTelemetry(t, (int) progress[0], progress[1] + 1000, () -> false)).isTrue();   // 재발행

        assertThat(outboxCount(flow)).isEqualTo(1);
        assertThat(aggregateCount(flow, 6101)).as("되감은 메시지는 상태에 다시 반영되지 않는다").isEqualTo(2);
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> STREAMS.commands(flow).size() == 1);
    }

    @Test
    @DisplayName("[FLW-05.02][AT-FLW-24.3] TC-FLW-093 히스테리시스 발생 상태에서 엔진 재적재(상태는 PostgreSQL) 후 26.5℃는 해제되지 않음, 해제 뒤 재발생은 1건 더")
    void stateSurvivesReload() {
        UUID flow = UUID.randomUUID();
        deploy(flow, 1, simple(6201, "{\"metric\":\"temperature\",\"op\":\">\",\"value\":27,\"clear\":26}", false, 6201));

        STREAMS.publish(temperature(6201, 28));
        await().atMost(Duration.ofSeconds(20)).until(() -> STREAMS.commands(flow).size() == 1);
        registry.remove(flow);                       // 메모리의 계획을 버리고
        synchronizer.syncAll(true);                  // core에서 다시 읽어 컴파일(재시작과 같은 경로)
        assertThat(registry.get(flow)).isPresent();
        STREAMS.publish(temperature(6201, 26.5));
        STREAMS.publish(temperature(6201, 28.5));
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> STREAMS.commands(flow).size() == 1);
        STREAMS.publish(temperature(6201, 25.0));
        STREAMS.publish(temperature(6201, 28.0));

        await().atMost(Duration.ofSeconds(20)).until(() -> STREAMS.commands(flow).size() == 2);
    }

    @Test
    @DisplayName("[FLW-05.02] TC-FLW-099 노드 상태·아웃박스는 한 트랜잭션: 커밋 직전 장애(아웃박스 INSERT 실패) 주입 → 재처리 뒤 상태·아웃박스 중복 0")
    void transactionAtomicity() {
        UUID flow = UUID.randomUUID();
        deploy(flow, 1, simple(6301, "{\"metric\":\"temperature\",\"op\":\">\",\"value\":27}", true, 6301));
        jdbc.sql("CREATE TABLE IF NOT EXISTS public.it_fault (flow_id uuid)").update();
        jdbc.sql("CREATE SEQUENCE IF NOT EXISTS public.it_fault_hits").update();
        jdbc.sql("""
                CREATE OR REPLACE FUNCTION public.it_fault_fn() RETURNS trigger AS $$
                BEGIN
                  IF EXISTS (SELECT 1 FROM public.it_fault WHERE flow_id = NEW.flow_id) THEN
                    PERFORM nextval('public.it_fault_hits');
                    RAISE EXCEPTION 'injected fault' USING ERRCODE = '40001';
                  END IF;
                  RETURN NEW;
                END $$ LANGUAGE plpgsql""").update();
        jdbc.sql("DROP TRIGGER IF EXISTS it_fault_trg ON data2flow_flow.flow_outboxes").update();
        jdbc.sql("CREATE TRIGGER it_fault_trg BEFORE INSERT ON data2flow_flow.flow_outboxes FOR EACH ROW EXECUTE FUNCTION public.it_fault_fn()").update();
        jdbc.sql("INSERT INTO public.it_fault VALUES (:f)").param("f", flow).update();
        long before = jdbc.sql("SELECT last_value FROM public.it_fault_hits").query(Long.class).single();

        STREAMS.publish(temperature(6301, 28));
        await().atMost(Duration.ofSeconds(20)).until(() -> jdbc.sql("SELECT last_value FROM public.it_fault_hits").query(Long.class).single() >= before + 2);
        assertThat(outboxCount(flow)).isZero();
        assertThat(aggregateCount(flow, 6301)).as("되돌려진 트랜잭션의 상태는 남지 않는다").isZero();
        jdbc.sql("DELETE FROM public.it_fault WHERE flow_id = :f").param("f", flow).update();

        await().atMost(Duration.ofSeconds(20)).until(() -> outboxCount(flow) == 1);
        assertThat(aggregateCount(flow, 6301)).isEqualTo(1);
        await().atMost(Duration.ofSeconds(20)).until(() -> STREAMS.commands(flow).size() == 1);
    }

    @Test
    @DisplayName("[FLW-05.03][AT-FLW-19.3] TC-FLW-103 플로우 A의 JS 무한 루프는 SCRIPT_ERROR로 끝나고 오류 지표 +1, 같은 기기를 보는 플로우 B는 모두 처리")
    void isolation() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        deploy(a, 1, FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["6401"]}}},
                  {"id":"n-js000001","type":"transform.js","config":{"code":"while (true) {}"}}],
                 "wires":[{"from":"n-trg00001","to":"n-js000001"}]}"""));
        deploy(b, 1, simple(6401, "{\"metric\":\"temperature\",\"op\":\">\",\"value\":27}", false, 6401));

        for (int i = 0; i < 10; i++) {
            STREAMS.publish(temperature(6401, 28 + i * 0.1));
        }

        await().atMost(Duration.ofSeconds(60)).until(() -> STREAMS.commands(b).size() == 10);
        await().atMost(Duration.ofSeconds(10)).until(() -> jdbc.sql(
                "SELECT coalesce(sum(errors), 0) FROM data2flow_flow.flow_metric_minutes WHERE flow_id = :f AND node_id = 'n-js000001'")
                .param("f", a).query(Long.class).single() == 10);
    }

    @Test
    @DisplayName("[FLW-01.06][FLW-06.01] 라이브 리로드: data2flow.config FLOW 변경을 받으면 새 버전(v2)을 컴파일해 바꿔 끼우고 적용 행이 v2가 된다")
    void liveReload() {
        UUID flow = UUID.randomUUID();
        deploy(flow, 1, simple(6501, "{\"metric\":\"temperature\",\"op\":\">\",\"value\":27}", false, 6501));

        TestInfrastructure.CORE.put(flow, FlowFixtures.ORG, 2, "ACTIVE",
                simple(6501, "{\"metric\":\"temperature\",\"op\":\">\",\"value\":30}", false, 6501));
        STREAMS.publishConfig(CODEC.write(new ConfigChangedMessage(1, UUID.randomUUID(), ConfigChangedMessage.EntityType.FLOW,
                flow.toString(), 2, ConfigChangedMessage.Op.UPSERT, "1", Instant.now(WALL))));

        await().atMost(Duration.ofSeconds(5)).until(() -> registry.get(flow).map(f -> f.version() == 2).orElse(false));
        await().atMost(Duration.ofSeconds(5)).until(() -> jdbc.sql(
                "SELECT applied_version FROM data2flow_flow.flow_instance_versions WHERE flow_id = :f").param("f", flow)
                .query(Integer.class).optional().orElse(0) == 2);
        STREAMS.publish(temperature(6501, 28));
        STREAMS.publish(temperature(6501, 31));
        await().atMost(Duration.ofSeconds(20)).until(() -> STREAMS.commands(flow).size() == 1);
        assertThat(STREAMS.commands(flow).getFirst().source().flowVersion()).isEqualTo(2);
    }
}
