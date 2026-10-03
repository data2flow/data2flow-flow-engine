package net.java21.data2flow.flow.runtime;

import net.java21.data2flow.flow.plan.domain.Overlay;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static net.java21.data2flow.flow.support.FlowTestHarness.errors;
import static net.java21.data2flow.flow.support.FlowTestHarness.ports;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * 실행기: TC-FLW-096·100(노드 상태 256KB, BR-FLW-29), TC-FLW-101(오류 격리, AT-FLW-19.3), 오류 포트, 한도(BR-FLW-16), 바이패스
 * (BR-FLW-10), "고온이면 냉방" 템플릿 전체 흐름(M3 시연 플로우).
 */
class FlowExecutionTest {

    @Test
    @DisplayName("[FLW-05.01][FLW-05.02][AT-FLW-24.2] 고온이면 냉방: 공간 평균 27℃ 초과 5분 → Thermostat.set(cool,24) 정확히 1건")
    void coolingTemplate() {
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.cooling(27, "PT5M", 24));

        h.send(FlowFixtures.temperature(1, 28.0, h.clock.instant()));
        h.send(FlowFixtures.temperature(2, 27.4, h.clock.instant().plusSeconds(1)));   // 평균 27.7
        assertThat(h.store.outbox()).isEmpty();
        h.advance(Duration.ofMinutes(3));
        h.send(FlowFixtures.temperature(1, 28.2, h.clock.instant()));
        h.advance(Duration.ofMinutes(2));

        assertThat(h.store.outbox()).singleElement().satisfies(o -> {
            assertThat(o.action().summary()).startsWith("Thermostat.set{mode=cool, targetTemperature=24");
            assertThat(o.nodeId()).isEqualTo("n-act00001");
        });
        h.advance(Duration.ofMinutes(30));
        assertThat(h.store.outbox()).hasSize(1);
    }

    @Test
    @DisplayName("[FLW-05.02] TC-FLW-096·100 BR-FLW-29 노드 상태가 대상 키당 256KB를 넘으면 error 포트 NODE_STATE_TOO_LARGE, 이전 상태 유지, 다음 메시지 계속")
    void stateTooLarge() {
        FlowTestHarness h = FlowTestHarness.node("transform.aggregate", "{\"window\":\"PT24H\",\"fn\":\"avg\",\"groupBy\":\"all\"}");
        StringBuilder payload = new StringBuilder("{");
        for (int i = 0; i < 100; i++) {
            payload.append(i == 0 ? "" : ",").append("\"metric_with_a_quite_long_name_").append(i).append("\":").append(i);
        }
        payload.append('}');
        int ok = 0;
        int tooLarge = 0;
        for (int i = 0; i < 40; i++) {
            var r = h.sendTo("n-test0001", "{\"deviceId\":\"%s\",\"measuredAt\":\"2026-03-02T00:%02d:00Z\",\"payload\":%s}"
                    .formatted("d".repeat(120), i, payload));
            if (errors(List.of(r), "n-test0001").isEmpty()) {
                ok++;
            } else {
                tooLarge++;
                assertThat(errors(List.of(r), "n-test0001").getFirst().errorType()).isEqualTo("NODE_STATE_TOO_LARGE");
            }
        }
        assertThat(ok).isPositive();
        assertThat(tooLarge).isPositive();
        assertThat(h.state("n-test0001", "all").toString().length()).isLessThanOrEqualTo(262_144);
    }

    @Test
    @DisplayName("[FLW-05.03][AT-FLW-19.3] TC-FLW-101 JS 노드 예외(error 포트·catch 없음): 그 갈래만 끝나고 다른 갈래·다음 메시지는 계속, 오류 지표 +1")
    void errorIsolation() {
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-js000001","type":"transform.js","config":{"code":"throw new Error('boom');"}},
                  {"id":"n-dbg00001","type":"debug.log","config":{}},
                  {"id":"n-dbg00002","type":"debug.log","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-js000001"},{"from":"n-js000001","to":"n-dbg00001"},
                          {"from":"n-trg00001","to":"n-dbg00002"}]}"""));

        var first = h.send(FlowFixtures.temperature(1, 25, h.clock.instant()));
        var second = h.send(FlowFixtures.temperature(1, 26, h.clock.instant()));

        assertThat(first.getFirst().errors()).isEqualTo(1);
        assertThat(errors(first, "n-js000001").getFirst().errorMessage()).contains("boom");
        assertThat(ports(first, "n-dbg00001")).isEmpty();
        assertThat(ports(first, "n-dbg00002")).containsExactly("out");
        assertThat(ports(second, "n-dbg00002")).containsExactly("out");
        assertThat(first.getFirst().metrics().get("n-js000001").errors()).isEqualTo(1);
        assertThat(first.getFirst().samples()).anySatisfy(s -> assertThat(s.port()).isEqualTo("error"));
    }

    @Test
    @DisplayName("[FLW-05.03] error 포트에 와이어가 있으면 {원래 메시지, error:{nodeId, errorType, message, attempts}}가 나간다")
    void errorPort() {
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-js000001","type":"transform.js","config":{"code":"throw new Error('boom');"}},
                  {"id":"n-dbg00001","type":"debug.log","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-js000001"},{"from":"n-js000001","port":"error","to":"n-dbg00001"}]}"""));

        var r = h.send(FlowFixtures.temperature(1, 25, h.clock.instant()));

        assertThat(FlowTestHarness.outputs(r, "n-js000001")).singleElement().satisfies(o -> {
            assertThat(o.port()).isEqualTo("error");
            assertThat(o.payload().path("error").path("nodeId").asString()).isEqualTo("n-js000001");
            assertThat(o.payload().path("error").path("errorType").asString()).isEqualTo("SCRIPT_ERROR");
            assertThat(o.payload().path("payload").path("temperature").asDouble()).isEqualTo(25);
        });
        assertThat(ports(r, "n-dbg00001")).containsExactly("out");
    }

    @Test
    @DisplayName("[FLW-10.02] BR-FLW-16 노드 하나가 입력 하나에 내보내는 메시지 100개 초과는 FLOW_FANOUT_LIMIT")
    void fanoutLimit() {
        List<net.java21.data2flow.flow.plan.domain.NodeType> types = new java.util.ArrayList<>(
                FlowTestHarness.registry(net.java21.data2flow.flow.node.service.SpaceDirectory.NONE).all());
        types.add(new Burst());
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-brst0001","type":"test.burst","config":{}},
                  {"id":"n-dbg00001","type":"debug.log","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-brst0001"},{"from":"n-brst0001","to":"n-dbg00001"}]}"""),
                new net.java21.data2flow.flow.plan.service.NodeTypeRegistry(types), 1);

        var r = h.send(FlowFixtures.temperature(1, 25, h.clock.instant()));

        assertThat(r.getFirst().steps()).anySatisfy(s -> assertThat(s.errorType()).isEqualTo("FLOW_FANOUT_LIMIT"));
        assertThat(ports(r, "n-dbg00001")).hasSize(100);
    }

    /** 입력 하나에 150건을 내는 시험용 노드 */
    static final class Burst implements net.java21.data2flow.flow.plan.domain.NodeType {
        @Override
        public net.java21.data2flow.contracts.flow.FlowNodeType descriptor() {
            var debug = FlowTestHarness.registry(net.java21.data2flow.flow.node.service.SpaceDirectory.NONE).find("debug.log")
                    .orElseThrow().descriptor();
            return new net.java21.data2flow.contracts.flow.FlowNodeType("test.burst", 1, "flow", "burst", null, null,
                    debug.configSchema(), debug.inputs(), debug.outputs(), debug.statePolicy(), List.of(), debug.defaults());
        }

        @Override
        public net.java21.data2flow.flow.plan.domain.CompiledNode compile(net.java21.data2flow.contracts.flow.FlowNode node,
                                                                       net.java21.data2flow.flow.plan.domain.CompileContext context) {
            return new net.java21.data2flow.flow.plan.domain.CompiledNode() {
                @Override
                public void onMessage(net.java21.data2flow.flow.plan.domain.FlowMessage message,
                                      net.java21.data2flow.flow.plan.domain.NodeContext ctx) {
                    for (int i = 0; i < 150; i++) {
                        ctx.emit("out", message.copy());
                    }
                }

                @Override
                public List<String> outputs() {
                    return List.of("out");
                }
            };
        }
    }

    @Test
    @DisplayName("[FLW-06.04] BR-FLW-10 바이패스된 조건 노드는 입력을 첫 출력 포트(true)로 넘긴다, overlay.debug 노드는 입출력 샘플")
    void bypassCondition() {
        FlowTestHarness h = FlowTestHarness.node("condition.threshold", "{\"metric\":\"temperature\",\"op\":\">\",\"value\":99}")
                .overlay(new Overlay(Set.of("n-test0001"), Set.of("n-test0001"), 1));

        var r = h.send(FlowFixtures.temperature(1, 20, h.clock.instant()));

        assertThat(ports(r, "n-test0001")).containsExactly("true");
        assertThat(r.getFirst().samples()).extracting(s -> s.direction()).contains("in", "out");
    }
}
