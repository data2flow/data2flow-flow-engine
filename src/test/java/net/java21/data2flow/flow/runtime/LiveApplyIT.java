package net.java21.data2flow.flow.runtime;

import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.IntegrationTestSupport;
import net.java21.data2flow.flow.support.TestInfrastructure;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** 라이브 적용 상태 이어받기 통합 시험(FLW-06.03·06.07, TC-FLW-141·144): 상태는 PostgreSQL 상태 행(지문 포함)으로 이어진다 */
class LiveApplyIT extends IntegrationTestSupport {

    private static final Clock WALL = Clock.systemUTC();

    @Autowired
    FlowRegistry registry;
    @Autowired
    JdbcClient jdbc;

    private void deploy(UUID flow, int version, FlowDefinition definition) {
        TestInfrastructure.CORE.put(flow, FlowFixtures.ORG, version, "ACTIVE", definition);
        await().atMost(Duration.ofSeconds(20)).until(() -> registry.get(flow).map(f -> f.version() == version).orElse(false));
    }

    private static FlowDefinition flow(long device, String metric, double value, String window) {
        return FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["%d"]}}},
                  {"id":"n-agg00001","type":"transform.aggregate","config":{"window":"%s","fn":"avg","groupBy":"device","metric":"%s"}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"%s","op":">","value":%s,"for":"PT10M","clear":%s}},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"deviceId":"%d"},"capability":"Switch","command":"set","args":{"on":true}}}],
                 "wires":[{"from":"n-trg00001","to":"n-agg00001"},{"from":"n-agg00001","to":"n-thr00001"},{"from":"n-thr00001","port":"true","to":"n-act00001"}]}"""
                .formatted(device, window, metric, metric, value, value - 1, device + 1));
    }

    private static CanonicalTelemetry reading(long device, String metric, double value) {
        Instant now = Instant.now(WALL);
        return CanonicalTelemetry.builder().organizationId(FlowFixtures.ORG).sourceId(3).externalId("la-" + device).deviceId(device)
                .modelId("la").measuredAt(now).receivedAt(now).metric(CanonicalTelemetry.Metric.of(metric, value, null)).rawMessageId(1).build();
    }

    private JsonNode state(UUID flow, String node, long device) {
        return jdbc.sql("""
                        SELECT state::text FROM data2flow_flow.flow_node_state WHERE flow_id = :f AND node_id = :n AND target_key = :k""")
                .param("f", flow).param("n", node).param("k", "device:" + device).query(String.class).optional()
                .map(Jsons.MAPPER::readTree).orElse(null);
    }

    private Instant waitingDue(UUID flow) {
        return jdbc.sql("SELECT due_at FROM data2flow_flow.flow_timers WHERE flow_id = :f AND status = 'WAITING'").param("f", flow)
                .query(java.sql.Timestamp.class).optional().map(java.sql.Timestamp::toInstant).orElse(null);
    }

    @Test
    @DisplayName("[FLW-06.03][AT-FLW-03.2·03.3] TC-FLW-141 KEEP(기준값)은 지속 타이머 due·집계 표본 그대로, MIGRATE(창 10m→15m)는 표본 보존, RESET(측정 항목)은 상태 행 삭제·판정 타이머 취소")
    void statePolicies() {
        UUID flow = UUID.randomUUID();
        long device = 8101;
        deploy(flow, 1, flow(device, "temperature", 27, "PT10M"));
        STREAMS.publish(reading(device, "temperature", 28));
        STREAMS.publish(reading(device, "temperature", 28.5));
        await().atMost(Duration.ofSeconds(20)).until(() -> waitingDue(flow) != null
                && state(flow, "n-agg00001", device) != null && state(flow, "n-agg00001", device).path("samples").size() == 2);
        Instant due = waitingDue(flow);

        deploy(flow, 2, flow(device, "temperature", 27.5, "PT10M"));      // KEEP
        assertThat(waitingDue(flow)).as("지속 타이머 그대로").isEqualTo(due);
        assertThat(state(flow, "n-thr00001", device).path("phase").asString()).isEqualTo("PENDING");

        deploy(flow, 3, flow(device, "temperature", 27.5, "PT15M"));      // MIGRATE(집계 창)
        STREAMS.publish(reading(device, "temperature", 29));
        await().atMost(Duration.ofSeconds(20)).until(() -> state(flow, "n-agg00001", device).path("samples").size() == 3);
        assertThat(waitingDue(flow)).isEqualTo(due);

        deploy(flow, 4, flow(device, "co2", 1000, "PT15M"));              // RESET(측정 항목)
        await().atMost(Duration.ofSeconds(10)).until(() -> state(flow, "n-thr00001", device) == null
                && state(flow, "n-agg00001", device) == null);
        assertThat(waitingDue(flow)).as("RESET 노드의 이전 판정 타이머 취소").isNull();
    }

    @Test
    @DisplayName("[FLW-06.07][AT-FLW-06.2] TC-FLW-144·154 BR-FLW-07 삭제한 노드의 상태는 24시간 보관되고, 그 노드가 있는 버전으로 롤백하면 복원되어 이어진다")
    void rollbackRestoresRemovedNodeState() {
        UUID flow = UUID.randomUUID();
        long device = 8201;
        deploy(flow, 12, flow(device, "temperature", 27, "PT10M"));
        STREAMS.publish(reading(device, "temperature", 28));
        await().atMost(Duration.ofSeconds(20)).until(() -> state(flow, "n-agg00001", device) != null);

        deploy(flow, 13, FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["%d"]}}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"temperature","op":">","value":27,"for":"PT10M","clear":26}}],
                 "wires":[{"from":"n-trg00001","to":"n-thr00001"}]}""".formatted(device)));
        assertThat(jdbc.sql("""
                        SELECT retain_until IS NOT NULL FROM data2flow_flow.flow_node_state WHERE flow_id = :f AND node_id = 'n-agg00001'""")
                .param("f", flow).query(Boolean.class).single()).as("24시간 보관 표시").isTrue();

        deploy(flow, 14, flow(device, "temperature", 27, "PT10M"));    // v12 내용으로 롤백(새 버전 v14)
        assertThat(jdbc.sql("""
                        SELECT retain_until IS NULL FROM data2flow_flow.flow_node_state WHERE flow_id = :f AND node_id = 'n-agg00001'""")
                .param("f", flow).query(Boolean.class).single()).as("복원").isTrue();
        STREAMS.publish(reading(device, "temperature", 29));
        await().atMost(Duration.ofSeconds(20)).until(() -> state(flow, "n-agg00001", device).path("samples").size() == 2);
    }
}
