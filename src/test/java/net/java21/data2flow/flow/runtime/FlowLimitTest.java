package net.java21.data2flow.flow.runtime;

import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.TimerKind;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static net.java21.data2flow.flow.support.FlowTestHarness.errors;
import static org.assertj.core.api.Assertions.assertThat;

/** 한도(FLW-10.02·10.03, BR-FLW-16) */
class FlowLimitTest {

    @Test
    @DisplayName("[FLW-10.03][AT-FLW-22.4] TC-FLW-220 플로우 대기 타이머 10,000개에서 새 대기 → error 포트 FLOW_TIMER_LIMIT, 기존 타이머는 그대로")
    void timerLimit() {
        FlowTestHarness h = FlowTestHarness.node("flow.delay", "{\"duration\":\"PT30S\"}");
        for (int i = 0; i < 10_000; i++) {
            h.store.insertTimer(FlowFixtures.FLOW, 1, 1, "n-test0001", "device:" + i, TimerKind.DELAY, Instant.MAX, Jsons.object());
        }
        var r = h.send(FlowFixtures.temperature(1, 25, h.clock.instant()));
        assertThat(errors(r, "n-test0001")).singleElement().satisfies(s -> assertThat(s.errorType()).isEqualTo("FLOW_TIMER_LIMIT"));
        assertThat(h.store.timers()).hasSize(10_000);
    }

    @Test
    @DisplayName("[FLW-10.02][AT-FLW-22.3] TC-FLW-217 메시지가 노드 100개를 거치면 다음 노드는 실행을 멈추고 error 포트 FLOW_HOP_LIMIT")
    void hopLimit() {
        StringBuilder nodes = new StringBuilder("{\"id\":\"n-trg00001\",\"type\":\"trigger.telemetry\",\"config\":{\"target\":{\"modelId\":\"em300-th\"}}}");
        StringBuilder wires = new StringBuilder();
        String prev = "n-trg00001";
        for (int i = 1; i <= 101; i++) {
            String id = String.format("n-map%05d", i);
            nodes.append(",{\"id\":\"").append(id).append("\",\"type\":\"transform.map\",\"config\":{\"rules\":[{\"op\":\"set\",\"path\":\"i\",\"value\":")
                    .append(i).append("}]}}");
            wires.append(wires.isEmpty() ? "" : ",").append("{\"from\":\"").append(prev).append("\",\"to\":\"").append(id).append("\"}");
            prev = id;
        }
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition("{\"schema\":\"data2flow.flow-definition/v1\",\"nodes\":[" + nodes
                + "],\"wires\":[" + wires + "]}"));
        var r = h.send(FlowFixtures.temperature(1, 25, h.clock.instant()));
        assertThat(errors(r, "n-map00100")).singleElement().satisfies(s -> assertThat(s.errorType()).isEqualTo("FLOW_HOP_LIMIT"));
        assertThat(r.getFirst().steps().stream().noneMatch(s -> s.nodeId().equals("n-map00101"))).isTrue();
    }
}
