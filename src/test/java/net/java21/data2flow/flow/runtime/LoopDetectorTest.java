package net.java21.data2flow.flow.runtime;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.event.FlowStateChanged;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.domain.Overlay;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.runtime.service.FlowSafetyGuard;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import net.java21.data2flow.flow.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** 안전장치(FLW-05.04, BR-FLW-14)와 오류율 감시(FLW-08.03, BR-FLW-26) */
class LoopDetectorTest {

    private final MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
    private final FlowRegistry registry = new FlowRegistry();
    private final List<FlowStateChanged> events = new ArrayList<>();
    private final FlowSafetyGuard guard = new FlowSafetyGuard(registry, (org, change) -> events.add(change), clock, 0.1);

    private LoadedFlow load(UUID flowId, int rateLimit) {
        var plan = FlowTestHarness.of(FlowFixtures.cooling(27, "PT5M", 24)).plan();
        LoadedFlow f = new LoadedFlow(flowId, FlowFixtures.ORG, "f", "ACTIVE", plan, Overlay.NONE, "FLOW", rateLimit, "DROP");
        registry.put(f);
        return f;
    }

    private static CanonicalTelemetry withLineage(String... flowIds) {
        var meta = new CanonicalTelemetry.Meta(null, null, null, Map.of("lineage", Jsons.MAPPER.valueToTree(List.of(flowIds))));
        return CanonicalTelemetry.builder().organizationId(1).sourceId(3).externalId("d").deviceId(1).modelId("em300-th")
                .measuredAt(MutableClock.T0).receivedAt(MutableClock.T0)
                .metric(CanonicalTelemetry.Metric.of("temperature", 28, "℃")).meta(meta).rawMessageId(1).build();
    }

    @Test
    @DisplayName("[FLW-05.04][AT-FLW-09.1] TC-FLW-106 계보에 같은 flowId가 다시 나타나면 순환(간접 A→B→A 포함) → PAUSED(CYCLE)와 상태 변경 1건")
    void cycle() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        LoadedFlow flowA = load(a, 100);

        assertThat(guard.cycle(flowA, withLineage(b.toString()))).isFalse();
        assertThat(guard.cycle(flowA, withLineage(a.toString(), b.toString()))).as("A→B→A").isTrue();

        assertThat(registry.get(a).orElseThrow().status()).isEqualTo("PAUSED");
        assertThat(events).singleElement().satisfies(e -> {
            assertThat(e.reason()).isEqualTo(FlowStateChanged.Reason.CYCLE);
            assertThat(e.to()).isEqualTo("PAUSED");
        });
        assertThat(guard.cycle(registry.get(a).orElseThrow(), withLineage(a.toString()))).isTrue();
        assertThat(events).as("이미 멈춘 플로우는 다시 알리지 않음").hasSize(1);
    }

    @Test
    @DisplayName("[FLW-05.04] TC-FLW-104 BR-FLW-14 초당 실행 한도(기본 100) 초과 → PAUSED(RUNAWAY), 다음 초에는 다시 셈")
    void rateLimit() {
        UUID id = UUID.randomUUID();
        LoadedFlow f = load(id, 100);
        for (int i = 0; i < 100; i++) {
            assertThat(guard.admit(f)).isTrue();
        }
        assertThat(guard.admit(f)).isFalse();
        assertThat(registry.get(id).orElseThrow().paused()).isTrue();
        assertThat(events).singleElement().satisfies(e -> {
            assertThat(e.reason()).isEqualTo(FlowStateChanged.Reason.RUNAWAY);
            assertThat(e.metrics().ratePerSec()).isEqualTo(101);
        });
        // core가 아직 모르면(같은 버전·ACTIVE) 멈춘 채로, PAUSED를 알려 주거나 새 버전이 오면 내려놓는다
        assertThat(guard.holdsPause(id, "ACTIVE", f.version())).isTrue();
        assertThat(guard.holdsPause(id, "PAUSED", f.version())).isFalse();
        assertThat(guard.holdsPause(id, "ACTIVE", f.version())).isFalse();

        UUID other = UUID.randomUUID();
        LoadedFlow g = load(other, 1000);
        clock.advance(Duration.ofSeconds(1));
        for (int i = 0; i < 1000; i++) {
            assertThat(guard.admit(g)).isTrue();
        }
    }

    private static ExecutionReport report(boolean error) {
        return new ExecutionReport(FlowFixtures.FLOW, 1, UUID.randomUUID().toString(), MutableClock.T0, 1, List.of(), List.of(),
                List.of(), error ? 1 : 0, Map.of());
    }

    @Test
    @DisplayName("[FLW-08.03][AT-FLW-10.3] TC-FLW-192·193 BR-FLW-26 최근 5분 100건 이상·오류율 15% → DEGRADED 1건, 10분 연속 기준 미만이면 RECOVERED")
    void degradedAndRecovered() {
        LoadedFlow f = load(FlowFixtures.FLOW, 1000);
        for (int i = 0; i < 99; i++) {
            guard.executed(f, report(i % 4 == 0));
        }
        guard.evaluate();
        assertThat(events).as("100건 미만이면 판정하지 않음").isEmpty();
        guard.executed(f, report(true));
        guard.evaluate();
        guard.evaluate();
        assertThat(events).singleElement().satisfies(e -> assertThat(e.reason()).isEqualTo(FlowStateChanged.Reason.DEGRADED));
        assertThat(guard.degraded(FlowFixtures.FLOW)).isTrue();
        assertThat(registry.get(FlowFixtures.FLOW).orElseThrow().running()).as("자동 정지가 아니면 계속 실행").isTrue();

        clock.advance(Duration.ofMinutes(6));      // 오류 창이 지나 기준 미만
        guard.evaluate();
        clock.advance(Duration.ofMinutes(9));
        guard.evaluate();
        assertThat(events).hasSize(1);
        clock.advance(Duration.ofMinutes(1));
        guard.evaluate();
        assertThat(events).hasSize(2);
        assertThat(events.getLast().reason()).isEqualTo(FlowStateChanged.Reason.RECOVERED);
        assertThat(guard.errorRate(FlowFixtures.FLOW)).isZero();
    }
}
