package net.java21.data2flow.flow.node;

import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;

import static net.java21.data2flow.flow.support.FlowTestHarness.outputs;
import static net.java21.data2flow.flow.support.FlowTestHarness.ports;
import static org.assertj.core.api.Assertions.assertThat;

/** FLW-02 {@code transform.aggregate}(TC-FLW-047, AT-FLW-24.5)·{@code flow.delay}(TC-FLW-049) */
class AggregateDelayNodeTest {

    private static final String NODE = "n-test0001";

    @ParameterizedTest(name = "fn={0} → {1}")
    @CsvSource({"avg, 27.5", "max, 29", "min, 26", "sum, 110", "count, 4"})
    @DisplayName("[FLW-05.02] TC-FLW-047 공간 단위 10분 창: 기기 4대 26·27·28·29 → 마지막 입력의 출력이 집계값, 대상 키 space:31")
    void aggregateBySpace(String fn, double expected) {
        FlowTestHarness h = FlowTestHarness.node("transform.aggregate",
                "{\"window\":\"PT10M\",\"fn\":\"%s\",\"groupBy\":\"space\"}".formatted(fn));
        Instant t = h.clock.instant();
        double[] values = {26, 27, 28, 29};
        var last = outputs(h.send(FlowFixtures.temperature(1, values[0], t)), NODE);
        for (int d = 1; d < 4; d++) {
            last = outputs(h.send(FlowFixtures.temperature(d + 1, values[d], t.plusSeconds(d))), NODE);
        }

        assertThat(last).singleElement().satisfies(o -> {
            assertThat(o.payload().path("payload").path("temperature").asDouble()).isEqualTo(expected);
            assertThat(o.payload().path("aggregate").path("count").asInt()).isEqualTo(4);
        });
        assertThat(h.state(NODE, "space:31")).isNotNull();
    }

    @Test
    @DisplayName("[FLW-05.02] TC-FLW-047 창 밖(10분 1초 전) 표본은 빠진다(측정 시각 기준)")
    void windowEviction() {
        FlowTestHarness h = FlowTestHarness.node("transform.aggregate", "{\"window\":\"10m\",\"fn\":\"avg\",\"groupBy\":\"device\"}");
        Instant t = h.clock.instant();

        h.send(FlowFixtures.temperature(1, 20, t));
        var out = outputs(h.send(FlowFixtures.temperature(1, 30, t.plus(Duration.ofMinutes(10)).plusSeconds(1))), NODE);

        assertThat(out.getFirst().payload().path("payload").path("temperature").asDouble()).isEqualTo(30);
    }

    @Test
    @DisplayName("[FLW-05.02] TC-FLW-049 delay 30s: 지속 타이머 1행, 29초까지 0건, 30초에 1건(받은 메시지 그대로)")
    void delay() {
        FlowTestHarness h = FlowTestHarness.node("flow.delay", "{\"duration\":\"PT30S\"}");

        h.send(FlowFixtures.temperature(1, 25, h.clock.instant()));
        assertThat(h.store.timers()).hasSize(1);
        assertThat(ports(h.advance(Duration.ofSeconds(29)), NODE)).isEmpty();
        var fired = h.advance(Duration.ofSeconds(1));

        assertThat(outputs(fired, NODE)).singleElement()
                .satisfies(o -> assertThat(o.payload().path("payload").path("temperature").asDouble()).isEqualTo(25));
        assertThat(h.store.timers()).isEmpty();
    }
}
