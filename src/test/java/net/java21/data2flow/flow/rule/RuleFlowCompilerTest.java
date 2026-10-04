package net.java21.data2flow.flow.rule;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.rule.service.RuleFlowCompiler;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 규칙 → 플로우 컴파일(RUL-01.01, BR-RUL-01·03·04·05, ADR-005) */
class RuleFlowCompilerTest {

    private final RuleFlowCompiler compiler = new RuleFlowCompiler();
    private final FlowCompiler flows = new FlowCompiler(FlowTestHarness.registry(SpaceDirectory.NONE));

    static String rule(String scope, String condition, String extra) {
        return """
                {"scope":%s,"condition":%s,"severity":"MAJOR","titleTemplate":"고CO2 {{value}}ppm"%s}""".formatted(scope, condition, extra);
    }

    private FlowDefinition compile(String json) {
        FlowDefinition d = compiler.compile(42, Jsons.MAPPER.readTree(json)).definition();
        assertThat(flows.compile(new UUID(0, 42), 1, 1, d).errors()).as("엔진 컴파일러 통과").isEmpty();
        return d;
    }

    private static FlowNode node(FlowDefinition d, String id) {
        return d.nodes().stream().filter(n -> n.id().equals(id)).findFirst().orElseThrow();
    }

    @Test
    @DisplayName("[RUL-01.01][AT-RUL-01.1] BR-RUL-01 임계값 규칙 → 트리거(범위) → 임계값(for·clear) → 알람 RAISE·CLEAR, 노드 ID는 역할별 고정, 알림 노드 없음")
    void threshold() {
        FlowDefinition d = compile(rule("{\"type\":\"SPACE\",\"ids\":[\"31\",\"32\"],\"includeChildren\":true}",
                "{\"kind\":\"threshold\",\"metric\":\"co2\",\"op\":\">\",\"value\":1000,\"for\":\"PT5M\",\"clear\":900}", ""));

        assertThat(d.nodes()).extracting(FlowNode::id).containsExactly("n-rtrg0001", "n-rtrg0002", "n-rcnd0001", "n-rrai0001", "n-rclr0001");
        assertThat(node(d, "n-rtrg0001").config().path("target").path("spaceId").asString()).isEqualTo("31");
        assertThat(node(d, "n-rtrg0001").config().path("metrics").get(0).asString()).isEqualTo("co2");
        assertThat(node(d, "n-rcnd0001").config().path("clear").asDouble()).isEqualTo(900);
        assertThat(node(d, "n-rrai0001").config().path("title").asString()).isEqualTo("고CO2 {{payload.co2}}ppm");
        assertThat(node(d, "n-rrai0001").config().path("ruleId").asString()).isEqualTo("42");
        assertThat(node(d, "n-rrai0001").config().path("threshold").path("raise").asDouble()).isEqualTo(1000);
        assertThat(d.wires()).anySatisfy(w -> {
            assertThat(w.from()).isEqualTo("n-rcnd0001");
            assertThat(w.port()).isEqualTo("false");
            assertThat(w.to()).isEqualTo("n-rclr0001");
        });
        assertThat(d.nodes()).noneMatch(n -> n.type().equals("action.notify"));
    }

    @Test
    @DisplayName("[RUL-01.03] BR-RUL-04 해제 기준이 없으면 발생 기준, 지속 시간이 없으면 0초(상태가 바뀔 때만 발생), autoClear=false면 해제 노드 없음")
    void defaults() {
        FlowDefinition d = compile(rule("{\"type\":\"DEVICE\",\"ids\":[\"7\"]}",
                "{\"kind\":\"threshold\",\"metric\":\"battery\",\"op\":\"<\",\"value\":20,\"repeat\":2}", ",\"autoClear\":false"));
        var c = node(d, "n-rcnd0001").config();
        assertThat(c.path("for").asString()).isEqualTo("PT0S");
        assertThat(c.path("clear").asDouble()).isEqualTo(20);
        assertThat(c.path("repeat").asInt()).isEqualTo(2);
        assertThat(d.nodes()).noneMatch(n -> n.id().equals("n-rclr0001"));
        assertThat(node(d, "n-rtrg0001").config().path("target").path("deviceIds").get(0).asString()).isEqualTo("7");
    }

    @Test
    @DisplayName("[RUL-01.01][AT-RUL-04.3] TC-RUL-018 공간 평균(spaceAvg) → 공간 집계 노드 → 임계값, 대상은 공간. any·all은 최대·최소로")
    void spaceAggregate() {
        var r = compiler.compile(1, Jsons.MAPPER.readTree(rule("{\"type\":\"SPACE\",\"ids\":[\"31\"]}",
                "{\"kind\":\"threshold\",\"metric\":\"co2\",\"op\":\">\",\"value\":1200,\"aggregate\":\"spaceAvg\"}", "")));
        assertThat(r.target()).isEqualTo("SPACE");
        assertThat(node(r.definition(), "n-ragg0001").config().path("fn").asString()).isEqualTo("avg");
        assertThat(node(r.definition(), "n-ragg0001").config().path("groupBy").asString()).isEqualTo("space");
        for (String[] c : List.of(new String[]{"any", ">", "max"}, new String[]{"all", ">", "min"}, new String[]{"any", "<", "min"},
                new String[]{"all", "<=", "max"}, new String[]{"spaceMax", ">", "max"}, new String[]{"spaceMin", ">", "min"})) {
            var x = compiler.compile(1, Jsons.MAPPER.readTree(rule("{\"type\":\"SPACE\",\"ids\":[\"31\"]}",
                    "{\"kind\":\"threshold\",\"metric\":\"co2\",\"op\":\"" + c[1] + "\",\"value\":1,\"aggregate\":\"" + c[0] + "\"}", "")));
            assertThat(node(x.definition(), "n-ragg0001").config().path("fn").asString()).as(c[0] + c[1]).isEqualTo(c[2]);
        }
    }

    @Test
    @DisplayName("[RUL-01.04][RUL-01.05][RUL-01.06][RUL-01.07] 변화율·무수신(timeout→발생, restored→해제)·복합(emit change)·시간 조건(발생만 거름)")
    void otherKinds() {
        FlowDefinition rate = compile(rule("{\"type\":\"MODEL\",\"ids\":[\"em300-th\"]}",
                "{\"kind\":\"rateOfChange\",\"metric\":\"temperature\",\"window\":\"PT10M\",\"delta\":3,\"direction\":\"UP\"}", ""));
        assertThat(node(rate, "n-rcnd0001").config().path("emit").asString()).isEqualTo("change");
        assertThat(node(rate, "n-rtrg0001").config().path("target").path("modelId").asString()).isEqualTo("em300-th");

        FlowDefinition noData = compile(rule("{\"type\":\"TAG\",\"ids\":[\"lab\"]}", "{\"kind\":\"noData\",\"window\":\"PT30M\"}", ""));
        assertThat(noData.wires()).anySatisfy(w -> assertThat(w.port() + ">" + w.to()).isEqualTo("timeout>n-rrai0001"));
        assertThat(noData.wires()).anySatisfy(w -> assertThat(w.port() + ">" + w.to()).isEqualTo("restored>n-rclr0001"));
        assertThat(node(noData, "n-rtrg0001").config().has("metrics")).as("무수신은 모든 메시지").isFalse();

        FlowDefinition group = compile(rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}",
                "{\"kind\":\"group\",\"op\":\"and\",\"items\":[{\"kind\":\"threshold\",\"metric\":\"co2\",\"op\":\">\",\"value\":1000},"
                        + "{\"kind\":\"threshold\",\"metric\":\"occupancy\",\"op\":\"inside\",\"range\":[1,100]}]}", ""));
        assertThat(node(group, "n-rcnd0001").config().path("op").asString()).isEqualTo("AND");
        assertThat(node(group, "n-rtrg0001").config().path("metrics").size()).isEqualTo(2);

        FlowDefinition timed = compile(rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}",
                "{\"kind\":\"threshold\",\"metric\":\"magnet\",\"op\":\"==\",\"value\":1}",
                ",\"timeCondition\":{\"days\":[\"SAT\",\"SUN\"],\"from\":\"18:00\",\"to\":\"09:00\",\"invert\":true}"));
        assertThat(timed.wires()).anySatisfy(w -> assertThat(w.from() + ":" + w.port() + ">" + w.to()).isEqualTo("n-rcnd0001:true>n-rtim0001"));
        assertThat(timed.wires()).anySatisfy(w -> assertThat(w.from() + ":" + w.port() + ">" + w.to()).isEqualTo("n-rtim0001:true>n-rrai0001"));
        assertThat(node(timed, "n-rtim0001").config().path("invert").asBoolean()).isTrue();
    }

    @Test
    @DisplayName("[RUL-01.01] RULE_CONDITION_INVALID: 범위·조건 종류·심각도·제목 오류, 이상 탐지(M7)·그룹 안 지속 시간·중첩 그룹·운영 시간표는 아직 거부")
    void invalid() {
        for (String json : List.of(
                rule("{\"type\":\"ROOM\",\"ids\":[\"1\"]}", "{\"kind\":\"threshold\",\"metric\":\"co2\",\"op\":\">\",\"value\":1}", ""),
                rule("{\"type\":\"DEVICE\",\"ids\":[]}", "{\"kind\":\"threshold\",\"metric\":\"co2\",\"op\":\">\",\"value\":1}", ""),
                rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}", "{\"kind\":\"anomaly\",\"minScore\":3}", ""),
                rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}", "{\"kind\":\"magic\"}", ""),
                rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}", "{\"kind\":\"threshold\",\"op\":\">\",\"value\":1}", ""),
                rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}", "{\"kind\":\"threshold\",\"metric\":\"co2\",\"op\":\">\"}", ""),
                rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}", "{\"kind\":\"threshold\",\"metric\":\"co2\",\"op\":\"==\",\"value\":1,\"aggregate\":\"any\"}", ""),
                rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}", "{\"kind\":\"threshold\",\"metric\":\"co2\",\"op\":\">\",\"value\":1,\"aggregate\":\"median\"}", ""),
                rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}", "{\"kind\":\"group\",\"items\":[{\"kind\":\"threshold\",\"metric\":\"a\",\"op\":\">\",\"value\":1,\"for\":\"PT5M\"}]}", ""),
                rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}", "{\"kind\":\"group\",\"items\":[{\"kind\":\"group\",\"items\":[]}]}", ""),
                rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}", "{\"kind\":\"group\",\"items\":[{\"kind\":\"threshold\",\"metric\":\"a\",\"op\":\">\",\"value\":1,\"aggregate\":\"spaceAvg\"}]}", ""),
                rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}", "{\"kind\":\"group\",\"items\":[]}", ""),
                rule("{\"type\":\"DEVICE\",\"ids\":[\"1\"]}", "{\"kind\":\"threshold\",\"metric\":\"co2\",\"op\":\">\",\"value\":1}",
                        ",\"timeCondition\":{\"spaceSchedule\":\"INSIDE\"}"),
                "{\"scope\":{\"type\":\"DEVICE\",\"ids\":[\"1\"]},\"condition\":{\"kind\":\"threshold\",\"metric\":\"co2\",\"op\":\">\",\"value\":1},\"severity\":\"LOUD\",\"titleTemplate\":\"\"}")) {
            assertThatThrownBy(() -> compiler.compile(1, Jsons.MAPPER.readTree(json))).as(json).isInstanceOf(BusinessException.class)
                    .hasMessageContaining("RULE_CONDITION_INVALID");
        }
        assertThatThrownBy(() -> compiler.compile(1, null)).isInstanceOf(BusinessException.class);
    }
}
