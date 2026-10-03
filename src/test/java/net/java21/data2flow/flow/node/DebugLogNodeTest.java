package net.java21.data2flow.flow.node;

import net.java21.data2flow.flow.plan.domain.Overlay;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static net.java21.data2flow.flow.support.FlowTestHarness.ports;
import static org.assertj.core.api.Assertions.assertThat;

/** FLW-02 {@code debug.log}(TC-FLW-059)와 바이패스(BR-FLW-10) */
class DebugLogNodeTest {

    private static final String NODE = "n-test0001";

    @Test
    @DisplayName("[FLW-05.01] TC-FLW-059 fields=[payload.temperature]: 샘플 1건(고른 필드), 메시지는 그대로 out")
    void sample() {
        FlowTestHarness h = FlowTestHarness.node("debug.log", "{\"level\":\"info\",\"fields\":[\"payload.temperature\"]}");

        var r = h.send(FlowFixtures.temperature(1, 25.5, h.clock.instant()));

        assertThat(ports(r, NODE)).containsExactly("out");
        assertThat(r.getFirst().samples()).singleElement().satisfies(s -> {
            assertThat(s.nodeId()).isEqualTo(NODE);
            assertThat(s.payload().path("data").path("payload.temperature").asDouble()).isEqualTo(25.5);
        });
    }

    @Test
    @DisplayName("[FLW-05.01] TC-FLW-059 노드를 끄면(disabled) 계획에 없어 발행 0, 바이패스된 행동 노드는 실행하지 않고 bypassed 기록")
    void disabledAndBypass() {
        FlowTestHarness off = FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1",
                 "nodes":[{"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                          {"id":"n-dbg00001","type":"debug.log","config":{},"disabled":true}],
                 "wires":[{"from":"n-trg00001","to":"n-dbg00001"}]}"""));
        assertThat(off.send(FlowFixtures.temperature(1, 25, off.clock.instant())).getFirst().samples()).isEmpty();

        FlowTestHarness bypass = FlowTestHarness.node("action.control",
                "{\"target\":{\"deviceId\":3},\"capability\":\"Switch\",\"command\":\"set\",\"args\":{\"on\":true}}")
                .overlay(new Overlay(Set.of(NODE), Set.of(), 2));
        var r = bypass.send(FlowFixtures.temperature(1, 25, bypass.clock.instant()));
        assertThat(bypass.store.outbox()).isEmpty();
        assertThat(r.getFirst().samples()).anySatisfy(s -> assertThat(s.payload().path("skipped").asString()).isEqualTo("bypassed"));
    }
}
