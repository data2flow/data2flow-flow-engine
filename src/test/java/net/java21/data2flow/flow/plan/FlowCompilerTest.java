package net.java21.data2flow.flow.plan;

import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.plan.domain.FlowValidationError;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * FLW-01.02·01.03 적용 전 검증(컴파일): API-FLW-06·07 {@code FLOW_VALIDATION_FAILED}의 {@code errors[{field, code, message}]}
 * (api-rules §5, field = {@code nodes[<id>].config.<경로>}·{@code wires[<i>]}), BR-FLW-02·03·04·05·16.
 */
class FlowCompilerTest {

    private final FlowCompiler compiler = new FlowCompiler(FlowTestHarness.registry(SpaceDirectory.NONE));

    private List<FlowValidationError> errors(String json) {
        return compiler.compile(FlowFixtures.FLOW, 1, 1, FlowFixtures.definition(json)).errors();
    }

    @Test
    @DisplayName("[FLW-01.05] 템플릿 \"고온이면 냉방\" 정의(웹 편집기 모양)는 오류 없이 4노드 계획이 된다")
    void coolingTemplateCompiles() {
        var result = compiler.compile(FlowFixtures.FLOW, 1, 3, FlowFixtures.cooling(27, "PT5M", 24));

        assertThat(result.errors()).isEmpty();
        assertThat(result.plan().nodes()).hasSize(4);
        assertThat(result.plan().triggers()).hasSize(1);
        assertThat(result.plan().ordered()).extracting(n -> n.id())
                .containsExactly("n-trg00001", "n-agg00001", "n-thr00001", "n-act00001");
    }

    @Test
    @DisplayName("[FLW-01.02] BR-FLW-05 설정 오류는 field=nodes[<id>].config.<경로>, code=INVALID_CONFIG")
    void invalidConfigField() {
        var e = errors("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":[1]}}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"temperature","op":">"}}],
                 "wires":[{"from":"n-trg00001","to":"n-thr00001"}]}""");

        assertThat(e).singleElement().isEqualTo(e.getFirst()).satisfies(x -> {
            assertThat(x.field()).isEqualTo("nodes[n-thr00001].config.value");
            assertThat(x.code()).isEqualTo("INVALID_CONFIG");
        });
    }

    @Test
    @DisplayName("[FLW-01.03] BR-FLW-03 없는 출력 포트는 wires[i].port UNKNOWN_PORT, 트리거로 들어가는 연결선은 TYPE_MISMATCH")
    void wireChecks() {
        var e = errors("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":[1]}}},
                  {"id":"n-trg00002","type":"trigger.telemetry","config":{"target":{"deviceIds":[2]}}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"op":">","value":1}}],
                 "wires":[{"from":"n-trg00001","to":"n-thr00001"},{"from":"n-thr00001","port":"maybe","to":"n-trg00002"},
                          {"from":"n-thr00001","port":"true","to":"n-trg00002"}]}""");

        assertThat(e).extracting(FlowValidationError::field, FlowValidationError::code).containsExactly(
                org.assertj.core.groups.Tuple.tuple("wires[1].port", "UNKNOWN_PORT"),
                org.assertj.core.groups.Tuple.tuple("wires[2]", "TYPE_MISMATCH"));
    }

    @Test
    @DisplayName("[FLW-01.01] BR-FLW-04 순환은 노드마다 CYCLE, BR-FLW-02 트리거가 없으면 NO_TRIGGER, 모르는 종류는 UNKNOWN_NODE_TYPE")
    void cycleNoTriggerUnknown() {
        var cycle = errors("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":[1]}}},
                  {"id":"n-map00001","type":"transform.map","config":{"rules":[{"op":"delete","path":"x"}]}},
                  {"id":"n-map00002","type":"transform.map","config":{"rules":[{"op":"delete","path":"y"}]}}],
                 "wires":[{"from":"n-trg00001","to":"n-map00001"},{"from":"n-map00001","to":"n-map00002"},{"from":"n-map00002","to":"n-map00001"}]}""");
        var noTrigger = errors("""
                {"schema":"data2flow.flow-definition/v1","nodes":[{"id":"n-map00001","type":"transform.map","config":{"rules":[{"op":"delete","path":"x"}]}}]}""");
        var unknown = errors("""
                {"schema":"data2flow.flow-definition/v1","nodes":[{"id":"n-ai000001","type":"ai.llmSummary","config":{}}]}""");

        assertThat(cycle).extracting(FlowValidationError::code).containsOnly("CYCLE").hasSize(2);
        assertThat(noTrigger).extracting(FlowValidationError::field, FlowValidationError::code)
                .containsExactly(org.assertj.core.groups.Tuple.tuple("nodes", "NO_TRIGGER"));
        assertThat(unknown).extracting(FlowValidationError::field).containsExactly("nodes[n-ai000001].type");
    }

    @Test
    @DisplayName("[FLW-01.01] 정의 구조 오류(스키마 표시·없는 노드 연결)는 field=definition, 노드 200개 초과는 LIMIT(BR-FLW-16)")
    void structureAndLimit() {
        var structure = errors("""
                {"schema":"other","nodes":[{"id":"n-trg00001","type":"trigger.telemetry","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-none0001"}]}""");
        StringBuilder nodes = new StringBuilder();
        for (int i = 0; i < 201; i++) {
            nodes.append(i == 0 ? "" : ",").append("{\"id\":\"n-map%05d\",\"type\":\"transform.map\",\"config\":{}}".formatted(i));
        }
        var limit = errors("{\"schema\":\"data2flow.flow-definition/v1\",\"nodes\":[" + nodes + "]}");

        assertThat(structure).extracting(FlowValidationError::field).containsOnly("definition");
        assertThat(limit).extracting(FlowValidationError::code).containsExactly("LIMIT");
    }

    @Test
    @DisplayName("[FLW-01.03] error 포트는 어느 노드 입력에나 이을 수 있고(error→message), JS 노드의 out은 out1로 정규화된다")
    void errorPortAndDefaultPort() {
        var result = compiler.compile(FlowFixtures.FLOW, 1, 1, FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":[1]}}},
                  {"id":"n-js000001","type":"transform.js","config":{"code":"return msg;","outputs":2}},
                  {"id":"n-dbg00001","type":"debug.log","config":{}},
                  {"id":"n-dbg00002","type":"debug.log","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-js000001"},{"from":"n-js000001","port":"out","to":"n-dbg00001"},
                          {"from":"n-js000001","port":"error","to":"n-dbg00002"}]}"""));

        assertThat(result.errors()).isEmpty();
        assertThat(result.plan().node("n-js000001").targets("out1")).containsExactly("n-dbg00001");
        assertThat(result.plan().node("n-js000001").targets("error")).containsExactly("n-dbg00002");
        assertThat(FlowCompiler.compatible("error", "message")).isTrue();
        assertThat(FlowCompiler.compatible("message", "error")).isFalse();
    }
}
