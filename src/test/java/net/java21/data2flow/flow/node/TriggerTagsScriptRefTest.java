package net.java21.data2flow.flow.node;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.node.service.ScriptDirectory;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import net.java21.data2flow.flow.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static net.java21.data2flow.flow.support.FlowTestHarness.outputs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 트리거 태그 대상(FLW-02 target.tags, M4)과 JS 함수 노드 스크립트 참조(scriptRef, M4) */
class TriggerTagsScriptRefTest {

    private static CanonicalTelemetry telemetry(long device, Map<String, String> metaTags) {
        return CanonicalTelemetry.builder().organizationId(1).sourceId(3).externalId("d" + device).deviceId(device).modelId("m")
                .measuredAt(MutableClock.T0).receivedAt(MutableClock.T0).metric(CanonicalTelemetry.Metric.of("temperature", 25, null))
                .meta(metaTags == null ? null : new CanonicalTelemetry.Meta(metaTags, null, null)).rawMessageId(1).build();
    }

    @Test
    @DisplayName("[FLW-02] TC-FLW-033 트리거 tags 대상: 기기 태그(core) 또는 메시지 meta.tags의 값·키:값이 겹치면 받고, 아니면 받지 않음")
    void tags() {
        SpaceDirectory spaces = new SpaceDirectory() {
            @Override
            public Set<Long> measuringDevices(long organizationId, long spaceId, boolean includeDescendants) {
                return Set.of();
            }

            @Override
            public Set<String> deviceTags(long organizationId, long deviceId) {
                return deviceId == 15 ? Set.of("lab") : Set.of();
            }
        };
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"tags":["lab","location:실습실"]}}}],"wires":[]}"""),
                spaces, 1);

        assertThat(h.send(telemetry(15, null))).hasSize(1);
        assertThat(h.send(telemetry(16, Map.of("location", "실습실")))).hasSize(1);
        assertThat(h.send(telemetry(17, Map.of("point", "lab")))).as("값이 태그와 같음").hasSize(1);
        assertThat(h.send(telemetry(18, Map.of("location", "복도")))).isEmpty();
        assertThatThrownBy(() -> FlowTestHarness.node("trigger.telemetry", "{\"target\":{\"tags\":[]}}"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"tags":["a"],"modelId":"m"}}}],"wires":[]}""")))
                .as("대상은 하나만").isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("[FLW-02][SCR-01] scriptRef s-12@v3: SCR 활성 버전 코드로 컴파일, 형식 오류·없는 버전은 config.scriptRef 오류")
    void scriptRef() {
        ScriptDirectory scripts = (org, id, version) -> id == 12 && version == 3
                ? Optional.of("return {...msg, payload:{temperature: msg.payload.temperature * 10}};") : Optional.empty();
        var registry = FlowTestHarness.registry(SpaceDirectory.NONE, scripts);
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-js000001","type":"transform.js","config":{"scriptRef":"s-12@v3"}}],
                 "wires":[{"from":"n-trg00001","to":"n-js000001"}]}"""), registry, 1);
        var r = h.send(FlowFixtures.temperature(1, 2.5, h.clock.instant()));
        assertThat(outputs(r, "n-js000001").getFirst().payload().path("payload").path("temperature").asDouble()).isEqualTo(25);

        for (String ref : new String[]{"s-12@v2", "script-12", "s-99@v1"}) {
            assertThatThrownBy(() -> FlowTestHarness.of(FlowFixtures.definition("""
                    {"schema":"data2flow.flow-definition/v1","nodes":[
                      {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                      {"id":"n-js000001","type":"transform.js","config":{"scriptRef":"%s"}}],
                     "wires":[{"from":"n-trg00001","to":"n-js000001"}]}""".formatted(ref)), registry, 1))
                    .as(ref).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("config.scriptRef");
        }
        ScriptDirectory failing = (org, id, version) -> {
            throw new IllegalStateException("core down");
        };
        assertThatThrownBy(() -> FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-js000001","type":"transform.js","config":{"scriptRef":"s-12@v3"}}],
                 "wires":[{"from":"n-trg00001","to":"n-js000001"}]}"""), FlowTestHarness.registry(SpaceDirectory.NONE, failing), 1))
                .hasMessageContaining("읽지 못했습니다");
    }
}
