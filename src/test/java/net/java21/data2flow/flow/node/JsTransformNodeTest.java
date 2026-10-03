package net.java21.data2flow.flow.node;

import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static net.java21.data2flow.flow.support.FlowTestHarness.errors;
import static net.java21.data2flow.flow.support.FlowTestHarness.outputs;
import static net.java21.data2flow.flow.support.FlowTestHarness.ports;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** FLW-02 {@code transform.js}: TC-FLW-046(GraalJS 샌드박스, SCR와 같은 제한), FLW-05.03 오류 격리 */
class JsTransformNodeTest {

    private static final String NODE = "n-test0001";

    private static String js(String code, int outputs) {
        return "{\"code\":" + net.java21.data2flow.flow.plan.domain.Jsons.MAPPER.writeValueAsString(code) + ",\"outputs\":" + outputs + "}";
    }

    @Test
    @DisplayName("[FLW-05.01] TC-FLW-046 본문 return: 반환 객체가 out1, payload 계산(보정)")
    void bodyReturn() {
        FlowTestHarness h = FlowTestHarness.node("transform.js",
                js("return {...msg, payload: {temperature: ctx.util.round(msg.payload.temperature + 0.55, 1)}};", 1));

        List<ExecutionReport> r = h.send(FlowFixtures.temperature(1, 24.0, h.clock.instant()));

        assertThat(outputs(r, NODE)).singleElement().satisfies(o -> {
            assertThat(o.port()).isEqualTo("out1");
            assertThat(o.payload().path("payload").path("temperature").asDouble()).isEqualTo(24.6);
        });
    }

    @Test
    @DisplayName("[FLW-05.01] TC-FLW-046 function main(msg) 정의만 있으면 main을 부르고, 숫자를 반환하면 payload가 그 값")
    void mainFunctionAndPrimitive() {
        FlowTestHarness h = FlowTestHarness.node("transform.js",
                js("function main(msg) { return msg.payload.co2 * 1.02; }", 1));

        ExecutionReport r = h.sendTo(NODE, "{\"messageId\":\"m-1\",\"payload\":{\"co2\":1000}}");

        assertThat(outputs(List.of(r), NODE).getFirst().payload().path("payload").asDouble()).isEqualTo(1020.0);
    }

    @Test
    @DisplayName("[FLW-01.03] TC-FLW-046 outputs=2, [msgA, null] → out1만 1건, 배열 원소가 배열이면 여러 건, 길이 3이면 error")
    void multipleOutputs() {
        FlowTestHarness h = FlowTestHarness.node("transform.js", js("return [msg, null];", 2));
        FlowTestHarness many = FlowTestHarness.node("transform.js", js("return [null, [msg, msg]];", 2));
        FlowTestHarness wrong = FlowTestHarness.node("transform.js", js("return [msg, msg, msg];", 2));

        assertThat(ports(List.of(h.sendTo(NODE, "{\"payload\":1}")), NODE)).containsExactly("out1");
        assertThat(ports(List.of(many.sendTo(NODE, "{\"payload\":1}")), NODE)).containsExactly("out2", "out2");
        assertThat(errors(List.of(wrong.sendTo(NODE, "{\"payload\":1}")), NODE)).singleElement()
                .satisfies(s -> assertThat(s.errorType()).isEqualTo("SCRIPT_ERROR"));
    }

    @Test
    @DisplayName("[FLW-05.03] TC-FLW-046 while(true)는 시간 제한으로 끊겨 error 포트 SCRIPT_ERROR, java.lang.System 접근 차단, 다음 실행은 정상")
    void sandboxLimits() {
        FlowTestHarness loop = FlowTestHarness.node("transform.js", js("while (true) {}", 1));
        FlowTestHarness host = FlowTestHarness.node("transform.js", js("return Java.type('java.lang.System').exit(1);", 1));
        FlowTestHarness ok = FlowTestHarness.node("transform.js", js("return msg;", 1));

        ExecutionReport timedOut = loop.sendTo(NODE, "{\"payload\":1}");
        ExecutionReport blocked = host.sendTo(NODE, "{\"payload\":1}");

        assertThat(errors(List.of(timedOut), NODE)).singleElement().satisfies(s -> {
            assertThat(s.errorType()).isEqualTo("SCRIPT_ERROR");
            assertThat(s.errorMessage()).contains("SCRIPT_TIMEOUT");
        });
        assertThat(errors(List.of(blocked), NODE)).singleElement()
                .satisfies(s -> assertThat(s.errorMessage()).contains("SCRIPT_FORBIDDEN_API"));
        assertThat(ports(List.of(ok.sendTo(NODE, "{\"payload\":1}")), NODE)).containsExactly("out1");
    }

    @Test
    @DisplayName("[FLW-01.02] 문법 오류·64KB 초과·scriptRef(M4)는 컴파일 오류(적용 막힘), null 반환은 출력 없음")
    void compileErrors() {
        assertThatThrownBy(() -> FlowTestHarness.node("transform.js", js("return {;", 1))).hasMessageContaining("INVALID_CONFIG");
        assertThatThrownBy(() -> FlowTestHarness.node("transform.js", js("//" + "x".repeat(70_000), 1))).hasMessageContaining("LIMIT");
        assertThatThrownBy(() -> FlowTestHarness.node("transform.js", "{\"scriptRef\":\"s-12@v3\"}")).hasMessageContaining("scriptRef");
        assertThatThrownBy(() -> FlowTestHarness.node("transform.js", js("return msg;", 11))).hasMessageContaining("config.outputs");

        FlowTestHarness h = FlowTestHarness.node("transform.js", js("return null;", 1));
        assertThat(ports(List.of(h.sendTo(NODE, "{\"payload\":1}")), NODE)).isEmpty();
    }

    @Test
    @DisplayName("[FLW-05.03] ctx는 읽기 전용 flow·node 정보, 호스트 객체 없음")
    void contextInfo() {
        FlowTestHarness h = FlowTestHarness.node("transform.js", js("return {flow: ctx.flow.id, node: ctx.node.id, v: ctx.flow.version};", 1));

        var out = outputs(List.of(h.sendTo(NODE, "{\"payload\":1}")), NODE).getFirst().payload();

        assertThat(out.path("flow").asString()).isEqualTo(FlowFixtures.FLOW.toString());
        assertThat(out.path("node").asString()).isEqualTo(NODE);
        assertThat(out.path("v").asInt()).isEqualTo(1);
    }
}
