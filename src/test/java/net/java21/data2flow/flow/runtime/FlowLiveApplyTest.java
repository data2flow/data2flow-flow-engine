package net.java21.data2flow.flow.runtime;

import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static net.java21.data2flow.flow.support.FlowTestHarness.ports;
import static org.assertj.core.api.Assertions.assertThat;

/** 라이브 적용 중 노드 상태 이어받기(FLW-06.03, BR-FLW-07, design §4.3): 계획만 바뀌고 상태는 저장소(지문)로 이어진다 */
class FlowLiveApplyTest {

    private static FlowDefinition threshold(String metric, double value, boolean mapBefore) {
        return FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  %s
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"%s","op":">","value":%s,"for":"PT5M","clear":%s}},
                  {"id":"n-dbg00001","type":"debug.log","config":{}}],
                 "wires":[%s{"from":"n-thr00001","port":"true","to":"n-dbg00001"}]}"""
                .formatted(mapBefore ? "{\"id\":\"n-map00001\",\"type\":\"transform.map\",\"config\":{\"rules\":[{\"op\":\"set\",\"path\":\"tag\",\"value\":\"x\"}]}}," : "",
                        metric, value, value - 1,
                        mapBefore ? "{\"from\":\"n-trg00001\",\"to\":\"n-map00001\"},{\"from\":\"n-map00001\",\"to\":\"n-thr00001\"},"
                                : "{\"from\":\"n-trg00001\",\"to\":\"n-thr00001\"},"));
    }

    @Test
    @DisplayName("[FLW-06.03][AT-FLW-03.2] TC-FLW-141 기준값만 바꾼(KEEP) v2를 3분째 적용해도 5분 지속 타이머가 이어져 2분 뒤 정확히 한 번 발생")
    void keepContinuesDurationTimer() {
        FlowTestHarness v1 = FlowTestHarness.of(threshold("temperature", 27, false));
        Instant t0 = v1.clock.instant();
        v1.send(FlowFixtures.temperature(1, 28.5, t0));
        v1.advance(Duration.ofMinutes(3));

        FlowTestHarness v2 = v1.apply(threshold("temperature", 28, false), 2);
        assertThat(v2.store.timers()).hasSize(1);
        var fired = v2.advance(Duration.ofMinutes(2));

        assertThat(ports(fired, "n-thr00001")).containsExactly("true");
        assertThat(fired.getFirst().version()).isEqualTo(2);
        assertThat(v2.store.timers()).isEmpty();
    }

    @Test
    @DisplayName("[FLW-06.03][AT-FLW-03.2] 판정 노드 앞에 변환 노드를 끼워 넣어도 \"5분 지속\" 타이머가 처음부터 다시 시작되지 않는다")
    void insertingNodeBeforeKeepsState() {
        FlowTestHarness v1 = FlowTestHarness.of(threshold("temperature", 27, false));
        v1.send(FlowFixtures.temperature(1, 28.5, v1.clock.instant()));
        v1.advance(Duration.ofMinutes(4));

        FlowTestHarness v2 = v1.apply(threshold("temperature", 27, true), 2);
        var fired = v2.advance(Duration.ofMinutes(1));

        assertThat(ports(fired, "n-thr00001")).containsExactly("true");
    }

    @Test
    @DisplayName("[FLW-06.03][AT-FLW-03.3] TC-FLW-137 측정 항목을 temperature→co2로 바꿔 적용 → 그 노드 상태 RESET(빈 상태로 다시 시작), 이전 타이머는 발화해도 아무것도 내지 않음")
    void metricChangeResetsState() {
        FlowTestHarness v1 = FlowTestHarness.of(threshold("temperature", 27, false));
        Instant t0 = v1.clock.instant();
        v1.send(FlowFixtures.temperature(1, 28.5, t0));
        assertThat(v1.state("n-thr00001", "device:1").path("phase").asString()).isEqualTo("PENDING");

        FlowTestHarness v2 = v1.apply(threshold("co2", 27, false), 2);
        v2.clock.advance(Duration.ofMinutes(1));
        v2.send(FlowFixtures.telemetry(1, FlowFixtures.SPACE, "co2", 30, v2.clock.instant()));

        var state = v2.state("n-thr00001", "device:1");
        assertThat(state.path("phase").asString()).isEqualTo("PENDING");
        assertThat(Instant.parse(state.path("since").asString())).as("새로 시작").isEqualTo(t0.plus(Duration.ofMinutes(1)));
        assertThat(v2.store.stored(FlowFixtures.FLOW, "n-thr00001", "device:1").stateConfig().path("metric").asString())
                .isEqualTo("co2");
        var old = v2.advance(Duration.ofMinutes(4));   // v1이 만든 타이머 만기(t0+5m)
        assertThat(ports(old, "n-thr00001")).as("RESET된 상태의 옛 타이머는 무시").isEmpty();
        var fired = v2.advance(Duration.ofMinutes(1)); // 새 대기 만기(t0+6m)
        assertThat(ports(fired, "n-thr00001")).containsExactly("true");
    }

    @Test
    @DisplayName("[FLW-06.03] TC-FLW-141 MIGRATE: 집계 창 10m→15m은 표본을 그대로, 15m→5m은 새 창으로 잘라 이어받는다")
    void aggregateWindowMigrates() {
        FlowDefinition w10 = aggregate("PT10M");
        FlowTestHarness v1 = FlowTestHarness.of(w10);
        Instant t0 = v1.clock.instant();
        for (int i = 0; i < 10; i++) {
            v1.send(FlowFixtures.temperature(1, 20 + i, t0.plus(Duration.ofMinutes(i))));
        }
        FlowTestHarness v2 = v1.apply(aggregate("PT15M"), 2);
        var r = v2.send(FlowFixtures.temperature(1, 30, t0.plus(Duration.ofMinutes(10))));
        assertThat(FlowTestHarness.outputs(r, "n-agg00001").getFirst().payload().path("aggregate").path("count").asInt())
                .as("10분치 9개(0~9분 중 창 안) + 새 값").isGreaterThanOrEqualTo(10);

        FlowTestHarness v3 = v2.apply(aggregate("PT5M"), 3);
        var r3 = v3.send(FlowFixtures.temperature(1, 31, t0.plus(Duration.ofMinutes(11))));
        assertThat(FlowTestHarness.outputs(r3, "n-agg00001").getFirst().payload().path("aggregate").path("count").asInt())
                .as("5분 창: 6~11분 표본만").isEqualTo(6);
    }

    private static FlowDefinition aggregate(String window) {
        return FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-agg00001","type":"transform.aggregate","config":{"window":"%s","fn":"avg","groupBy":"device","metric":"temperature"}}],
                 "wires":[{"from":"n-trg00001","to":"n-agg00001"}]}""".formatted(window));
    }
}
