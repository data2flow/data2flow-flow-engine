package net.java21.data2flow.flow.liveview;

import net.java21.data2flow.contracts.message.FlowDebugMessage;
import net.java21.data2flow.flow.liveview.service.LiveStatsCollector;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.domain.Overlay;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import net.java21.data2flow.flow.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** 노드 카운터·상태 배지(FLW-03.01·03.03, EVT-FLW-01 node.stats) */
class NodeHealthBadgeTest {

    private final MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
    private final LiveStatsCollector collector = new LiveStatsCollector(clock, "engine-1");
    private final LoadedFlow flow = new LoadedFlow(FlowFixtures.FLOW, FlowFixtures.ORG, "f", "ACTIVE",
            FlowTestHarness.of(FlowFixtures.cooling(27, "PT5M", 24)).plan(), Overlay.NONE);

    private ExecutionReport report(boolean error, String message) {
        List<ExecutionReport.Step> steps = List.of(
                new ExecutionReport.Step("n-trg00001", "trigger.telemetry", 0, 5, null, List.of("out"), null, null, List.of(), List.of()),
                new ExecutionReport.Step("n-thr00001", "condition.threshold", 5, 5, null, error ? List.of("error") : List.of("true"),
                        error ? "TYPE_MISMATCH" : null, message, List.of(), List.of()));
        return new ExecutionReport(FlowFixtures.FLOW, 4, "m", clock.instant(), 10, steps, List.of(), List.of(), error ? 1 : 0, Map.of());
    }

    @Test
    @DisplayName("[FLW-03.03] TC-FLW-069 최근 5분 오류 0 → OK, 오류율 1% 이상 → WARN, 마지막 실행이 오류이거나 10% 이상 → ERROR + 마지막 오류(500자)")
    void badges() {
        for (int i = 0; i < 98; i++) {
            collector.executed(flow, report(false, null));
        }
        assertThat(collector.status(FlowFixtures.FLOW, "n-thr00001")).isEqualTo(FlowDebugMessage.NodeStatus.OK);
        collector.executed(flow, report(true, "x".repeat(800)));
        assertThat(collector.status(FlowFixtures.FLOW, "n-thr00001")).as("마지막 실행 오류").isEqualTo(FlowDebugMessage.NodeStatus.ERROR);
        collector.executed(flow, report(false, null));
        assertThat(collector.status(FlowFixtures.FLOW, "n-thr00001")).as("1/100 = 1%").isEqualTo(FlowDebugMessage.NodeStatus.WARN);
        for (int i = 0; i < 20; i++) {
            collector.executed(flow, report(true, "boom"));
        }
        collector.executed(flow, report(false, null));
        assertThat(collector.status(FlowFixtures.FLOW, "n-thr00001")).as("21/121 ≥ 10%").isEqualTo(FlowDebugMessage.NodeStatus.ERROR);
        clock.advance(Duration.ofMinutes(6));
        assertThat(collector.status(FlowFixtures.FLOW, "n-thr00001")).as("5분이 지나면 OK").isEqualTo(FlowDebugMessage.NodeStatus.OK);
    }

    @Test
    @DisplayName("[FLW-03.01][AT-FLW-04.1] TC-FLW-060 1초마다 node.stats: 노드별 in·포트별 out·errors·lastAt·status·lastError, 활동이 없는 플로우는 내지 않음")
    void flushStats() {
        collector.executed(flow, report(false, null));
        collector.executed(flow, report(true, "y".repeat(600)));
        List<FlowDebugMessage> sent = new ArrayList<>();

        assertThat(collector.flush(sent::add)).isEqualTo(1);
        FlowDebugMessage m = sent.getFirst();
        assertThat(m.type()).isEqualTo(FlowDebugMessage.Type.NODE_STATS);
        assertThat(m.version()).isEqualTo(4);
        assertThat(m.routingKey()).isEqualTo("flow." + FlowFixtures.FLOW);
        FlowDebugMessage.NodeStats thr = m.stats().stream().filter(s -> s.nodeId().equals("n-thr00001")).findFirst().orElseThrow();
        assertThat(thr.in()).isEqualTo(2);
        assertThat(thr.out()).containsEntry("true", 1L).containsEntry("error", 1L);
        assertThat(thr.errors()).isEqualTo(1);
        assertThat(thr.status()).isEqualTo(FlowDebugMessage.NodeStatus.ERROR);
        assertThat(thr.lastError()).hasSize(500);

        assertThat(collector.flush(sent::add)).as("새 활동이 없으면 내지 않음").isZero();
        clock.advance(Duration.ofMinutes(10));
        assertThat(collector.flush(sent::add)).isZero();
    }
}
