package net.java21.data2flow.flow.runtime;

import net.java21.data2flow.flow.plan.domain.NodeConfigException;
import net.java21.data2flow.flow.plan.domain.RetryPolicy;
import net.java21.data2flow.flow.plan.domain.TimerKind;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static net.java21.data2flow.flow.support.FlowTestHarness.errors;
import static net.java21.data2flow.flow.support.FlowTestHarness.outputs;
import static net.java21.data2flow.flow.support.FlowTestHarness.ports;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 노드 재시도(FLW-08.01, BR-FLW-21)와 오류 포트(FLW-08.02) */
class RetryPolicyTest {

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
            "일반 노드 기본 0회||{\"retry\":{\"maxAttempts\":0}}|0|1000|2000",
            "행동 노드 기본 3회 1s·2배|null|{\"retry\":{\"maxAttempts\":3,\"backoff\":{\"initialMs\":1000,\"multiplier\":2,\"maxMs\":30000}}}|3|1000|2000",
            "정의가 기본값을 덮음|{\"maxAttempts\":5,\"backoff\":{\"initialMs\":500}}|{\"retry\":{\"maxAttempts\":3}}|5|500|1000"})
    @DisplayName("[FLW-08.01] TC-FLW-189 BR-FLW-21 재시도 기본값: 일반 0회, 행동 3회(지수 백오프 1s·2배·최대 30s), 노드 정의가 덮어씀")
    void defaults(String name, String nodeRetry, String typeDefaults, int attempts, long first, long second) {
        RetryPolicy p = RetryPolicy.of(nodeRetry == null || nodeRetry.isEmpty() || nodeRetry.equals("null") ? null
                : Jsons.MAPPER.readTree(nodeRetry), Jsons.MAPPER.readTree(typeDefaults));
        assertThat(p.maxAttempts()).isEqualTo(attempts);
        assertThat(p.delayAfter(1)).isEqualTo(Duration.ofMillis(first));
        assertThat(p.delayAfter(2)).isEqualTo(Duration.ofMillis(second));
        assertThat(p.delayAfter(20)).isLessThanOrEqualTo(Duration.ofSeconds(30));
    }

    @Test
    @DisplayName("[FLW-08.01] BR-FLW-21 재시도 정책이 범위를 벗어나면 retry 경로의 설정 오류")
    void invalid() {
        assertThatThrownBy(() -> RetryPolicy.of(Jsons.MAPPER.readTree("{\"maxAttempts\":11}"), null))
                .isInstanceOf(NodeConfigException.class).hasMessageContaining("0~10");
        assertThatThrownBy(() -> RetryPolicy.of(Jsons.MAPPER.readTree("{\"maxAttempts\":1,\"backoff\":{\"initialMs\":10}}"), null))
                .isInstanceOf(NodeConfigException.class);
    }

    @Test
    @DisplayName("[FLW-08.01][FLW-08.02][AT-FLW-19.1] TC-FLW-187 재시도 2회 노드가 계속 실패 → 1s·2s 뒤 다시 실행하고 마지막에 error 포트로 {원래 메시지, nodeId, errorType, attempts=3}")
    void retriesThenErrorPort() {
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-js000001","type":"transform.js","config":{"code":"throw new Error('down');"},"retry":{"maxAttempts":2,"backoff":{"initialMs":1000,"multiplier":2,"maxMs":30000}}},
                  {"id":"n-dbg00001","type":"debug.log","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-js000001"},{"from":"n-js000001","port":"error","to":"n-dbg00001"}]}"""));

        var first = h.send(FlowFixtures.temperature(1, 25, h.clock.instant()));
        assertThat(ports(first, "n-js000001")).containsExactly("retry");
        assertThat(first.getFirst().errors()).as("재시도 예약은 오류로 세지 않음").isZero();
        assertThat(h.store.timers().values()).singleElement().satisfies(t -> {
            assertThat(t.kind()).isEqualTo(TimerKind.RETRY);
            assertThat(t.dueAt()).isEqualTo(h.clock.instant().plusSeconds(1));
        });

        var second = h.advance(Duration.ofSeconds(1));
        assertThat(ports(second, "n-js000001")).containsExactly("retry");
        var third = h.advance(Duration.ofSeconds(2));

        assertThat(outputs(third, "n-js000001")).singleElement().satisfies(o -> {
            assertThat(o.port()).isEqualTo("error");
            assertThat(o.payload().path("error").path("attempts").asInt()).isEqualTo(3);
            assertThat(o.payload().path("error").path("errorType").asString()).isEqualTo("SCRIPT_ERROR");
            assertThat(o.payload().path("payload").path("temperature").asDouble()).isEqualTo(25);
        });
        assertThat(ports(third, "n-dbg00001")).containsExactly("out");
        assertThat(third.getFirst().errors()).isEqualTo(1);
        assertThat(h.store.timers()).isEmpty();
    }

    @Test
    @DisplayName("[FLW-08.01][AT-FLW-19.2] TC-FLW-188 재시도 중인 메시지가 같은 기기의 다음 메시지를 막지 않는다(재시도는 타이머로 미룸)")
    void retryDoesNotBlockNextMessage() {
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-js000001","type":"transform.js","config":{"code":"if (msg.payload.temperature > 30) throw new Error('hot'); return msg;"},"retry":{"maxAttempts":3}},
                  {"id":"n-dbg00001","type":"debug.log","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-js000001"},{"from":"n-js000001","to":"n-dbg00001"}]}"""));

        h.send(FlowFixtures.temperature(1, 35, h.clock.instant()));
        var next = h.send(FlowFixtures.temperature(1, 25, h.clock.instant()));

        assertThat(ports(next, "n-dbg00001")).containsExactly("out");
        assertThat(h.store.timers()).hasSize(1);
    }

    @Test
    @DisplayName("[FLW-08.02] TC-FLW-190 다시 해도 같은 오류(TYPE_MISMATCH)는 재시도 없이 error 포트, 연결이 없으면 기록과 지표만(오류 +1, 디버그 샘플)")
    void notRetryableAndUnwired() {
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-js000001","type":"transform.js","config":{"code":"return {...msg, payload:{temperature:'hot'}};"}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"temperature","op":">","value":27},"retry":{"maxAttempts":3}}],
                 "wires":[{"from":"n-trg00001","to":"n-js000001"},{"from":"n-js000001","to":"n-thr00001"}]}"""));

        var r = h.send(FlowFixtures.temperature(1, 25, h.clock.instant()));

        assertThat(errors(r, "n-thr00001")).singleElement().satisfies(s -> assertThat(s.errorType()).isEqualTo("TYPE_MISMATCH"));
        assertThat(h.store.timers()).isEmpty();
        assertThat(r.getFirst().metrics().get("n-thr00001").errors()).isEqualTo(1);
        List<String> samplePorts = new ArrayList<>();
        r.getFirst().samples().forEach(s -> samplePorts.add(s.port()));
        assertThat(samplePorts).contains("error");
    }
}
