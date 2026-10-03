package net.java21.data2flow.flow.node;

import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static net.java21.data2flow.flow.support.FlowTestHarness.errors;
import static net.java21.data2flow.flow.support.FlowTestHarness.ports;
import static org.assertj.core.api.Assertions.assertThat;

/** FLW-02 {@code condition.threshold}: TC-FLW-039(경계값), FLW-05.02 지속 시간·히스테리시스(AT-FLW-24.2·24.3 단위) */
class ThresholdConditionNodeTest {

    private static final String NODE = "n-test0001";

    @ParameterizedTest(name = "{0} {1} 기준 27 → {2}")
    @CsvSource({
            ">, 27.0, false", ">, 27.1, true", ">=, 27.0, true", ">=, 26.9, false",
            "<, 27.0, false", "<, 26.9, true", "<=, 27.0, true", "<=, 27.1, false",
            "==, 27.0, true", "==, 27.1, false", "!=, 27.0, false", "!=, 26.0, true"})
    @DisplayName("[FLW-05.01] TC-FLW-039 상태 없는 판정: 연산 × 경계값이 메시지마다 true/false 포트 하나로 나간다")
    void comparisonTable(String op, double value, boolean expected) {
        FlowTestHarness h = FlowTestHarness.node("condition.threshold", "{\"metric\":\"temperature\",\"op\":\"%s\",\"value\":27}".formatted(op));

        List<ExecutionReport> r = h.send(FlowFixtures.temperature(1, value, MutableTime.T0));

        assertThat(ports(r, NODE)).containsExactly(Boolean.toString(expected));
    }

    @ParameterizedTest(name = "{0} [18,28] {1} → {2}")
    @CsvSource({"outside, 17.9, true", "outside, 18.0, false", "outside, 28.0, false", "outside, 28.1, true",
            "inside, 18.0, true", "inside, 28.0, true", "inside, 17.9, false", "inside, 28.1, false"})
    @DisplayName("[FLW-05.01] TC-FLW-039 범위 연산 outside·inside는 경계를 포함해 판정한다")
    void rangeTable(String op, double value, boolean expected) {
        FlowTestHarness h = FlowTestHarness.node("condition.threshold",
                "{\"metric\":\"temperature\",\"op\":\"%s\",\"range\":[18,28]}".formatted(op));

        assertThat(ports(h.send(FlowFixtures.temperature(1, value, MutableTime.T0)), NODE)).containsExactly(Boolean.toString(expected));
    }

    @Test
    @DisplayName("[FLW-05.03] TC-FLW-039 숫자가 아닌 값은 error 포트(TYPE_MISMATCH), 측정 항목이 없으면 아무것도 내보내지 않는다")
    void nonNumericIsError() {
        FlowTestHarness h = FlowTestHarness.node("condition.threshold", "{\"metric\":\"temperature\",\"op\":\">\",\"value\":27}");

        ExecutionReport r = h.sendTo(NODE, "{\"deviceId\":1,\"payload\":{\"temperature\":\"hot\"}}");
        ExecutionReport missing = h.sendTo(NODE, "{\"deviceId\":1,\"payload\":{\"humidity\":40}}");

        assertThat(errors(List.of(r), NODE)).singleElement().satisfies(s -> assertThat(s.errorType()).isEqualTo("TYPE_MISMATCH"));
        assertThat(ports(List.of(missing), NODE)).isEmpty();
    }

    @Test
    @DisplayName("[FLW-05.02][AT-FLW-24.2] 27℃ 초과 5분 지속: 지속 타이머가 5분째에 true를 정확히 한 번 낸다(메시지 없이)")
    void durationFiresOnceByTimer() {
        FlowTestHarness h = FlowTestHarness.node("condition.threshold",
                "{\"metric\":\"temperature\",\"op\":\">\",\"value\":27,\"for\":\"PT5M\"}");

        List<ExecutionReport> first = h.send(FlowFixtures.temperature(1, 28.0, h.clock.instant()));
        assertThat(ports(first, NODE)).isEmpty();
        assertThat(h.store.timers()).hasSize(1);

        assertThat(ports(h.advance(Duration.ofMinutes(3)), NODE)).isEmpty();
        assertThat(ports(h.advance(Duration.ofSeconds(119)), NODE)).isEmpty();
        List<ExecutionReport> fired = h.advance(Duration.ofSeconds(1));
        assertThat(ports(fired, NODE)).containsExactly("true");
        assertThat(h.state(NODE, "device:1").path("phase").asString()).isEqualTo("ACTIVE");

        // 다시 참이 와도 ACTIVE 동안은 내보내지 않는다
        assertThat(ports(h.send(FlowFixtures.temperature(1, 28.5, h.clock.instant())), NODE)).isEmpty();
        assertThat(ports(h.advance(Duration.ofMinutes(10)), NODE)).isEmpty();
    }

    @Test
    @DisplayName("[FLW-05.02] 지속 중 거짓 메시지가 오면 대기를 버리고 타이머를 취소한다(발생 0)")
    void falseCancelsPending() {
        FlowTestHarness h = FlowTestHarness.node("condition.threshold",
                "{\"metric\":\"temperature\",\"op\":\">\",\"value\":27,\"for\":\"PT5M\"}");

        h.send(FlowFixtures.temperature(1, 28.0, h.clock.instant()));
        h.advance(Duration.ofMinutes(2));
        h.send(FlowFixtures.temperature(1, 26.0, h.clock.instant()));

        assertThat(h.store.cancelled()).hasSize(1);
        assertThat(ports(h.advance(Duration.ofMinutes(10)), NODE)).isEmpty();
    }

    @Test
    @DisplayName("[FLW-05.02][FLW-03.07] 측정 시각 기준으로도 판정한다: x60 가속 데이터(측정 1분 간격, 실제 1초)는 시뮬레이션 5분째에 한 번, 타이머는 취소")
    void eventTimeDuration() {
        FlowTestHarness h = FlowTestHarness.node("condition.threshold",
                "{\"metric\":\"temperature\",\"op\":\">\",\"value\":27,\"for\":\"PT5M\"}");
        Instant sim = Instant.parse("2026-08-10T14:00:00Z");

        int emitted = 0;
        for (int minute = 0; minute <= 6; minute++) {
            emitted += ports(h.send(FlowFixtures.temperature(1, 28.0, sim.plusSeconds(60L * minute))), NODE).size();
            h.clock.advance(Duration.ofSeconds(1));
        }

        assertThat(emitted).isEqualTo(1);
        assertThat(h.store.timers()).isEmpty();
        assertThat(ports(h.advance(Duration.ofMinutes(10)), NODE)).isEmpty();
    }

    @Test
    @DisplayName("[FLW-05.02][AT-FLW-24.3] 히스테리시스(발생 27, 해제 26): 발생 뒤 26.5는 해제되지 않고 26.0에서 false 한 번")
    void hysteresis() {
        FlowTestHarness h = FlowTestHarness.node("condition.threshold",
                "{\"metric\":\"temperature\",\"op\":\">\",\"value\":27,\"clear\":26}");

        assertThat(ports(h.send(FlowFixtures.temperature(1, 27.5, h.clock.instant())), NODE)).containsExactly("true");
        assertThat(ports(h.send(FlowFixtures.temperature(1, 26.5, h.clock.instant())), NODE)).isEmpty();
        assertThat(ports(h.send(FlowFixtures.temperature(1, 26.0, h.clock.instant())), NODE)).containsExactly("false");
        assertThat(ports(h.send(FlowFixtures.temperature(1, 26.9, h.clock.instant())), NODE)).isEmpty();
    }

    @Test
    @DisplayName("[FLW-05.02] 대상 키(기기)마다 상태가 따로다")
    void statePerTarget() {
        FlowTestHarness h = FlowTestHarness.node("condition.threshold", "{\"metric\":\"temperature\",\"op\":\">\",\"value\":27,\"clear\":26}");

        assertThat(ports(h.send(FlowFixtures.temperature(1, 28, h.clock.instant())), NODE)).containsExactly("true");
        assertThat(ports(h.send(FlowFixtures.temperature(2, 28, h.clock.instant())), NODE)).containsExactly("true");
        assertThat(h.state(NODE, "device:2").path("phase").asString()).isEqualTo("ACTIVE");
    }

    /** 시험 기준 시각 */
    static final class MutableTime {
        static final Instant T0 = Instant.parse("2026-03-02T00:00:00Z");
    }
}
