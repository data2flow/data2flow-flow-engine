package net.java21.data2flow.flow.node;

import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static net.java21.data2flow.flow.support.FlowTestHarness.outputs;
import static net.java21.data2flow.flow.support.FlowTestHarness.ports;
import static org.assertj.core.api.Assertions.assertThat;

/** FLW-02 {@code condition.switch}(TC-FLW-044, FLW-01.03 분기)·{@code transform.map}(TC-FLW-048) */
class SwitchMapNodeTest {

    private static final String NODE = "n-test0001";

    @Test
    @DisplayName("[FLW-01.03] TC-FLW-044 $.payload.mode: cool·heat는 각 케이스 포트 1건, 정의 없는 dry와 경로 없음은 default")
    void switchCases() {
        FlowTestHarness h = FlowTestHarness.node("condition.switch", """
                {"expression":"$.payload.mode","cases":[{"name":"cool","op":"==","value":"cool"},{"name":"heat","op":"==","value":"heat"}]}""");

        assertThat(ports(List.of(h.sendTo(NODE, "{\"payload\":{\"mode\":\"cool\"}}")), NODE)).containsExactly("cool");
        assertThat(ports(List.of(h.sendTo(NODE, "{\"payload\":{\"mode\":\"heat\"}}")), NODE)).containsExactly("heat");
        assertThat(ports(List.of(h.sendTo(NODE, "{\"payload\":{\"mode\":\"dry\"}}")), NODE)).containsExactly("default");
        assertThat(ports(List.of(h.sendTo(NODE, "{\"payload\":{}}")), NODE)).containsExactly("default");
    }

    @Test
    @DisplayName("[FLW-01.03] 스위치 숫자·in·exists 연산과 처음 맞는 케이스 하나로만 나감")
    void switchOps() {
        FlowTestHarness h = FlowTestHarness.node("condition.switch", """
                {"expression":"payload.co2","cases":[{"name":"high","op":">=","value":1500},{"name":"warn","op":">","value":1000},
                 {"name":"listed","op":"in","value":[400,500]},{"name":"any","op":"exists"}]}""");

        assertThat(ports(List.of(h.sendTo(NODE, "{\"payload\":{\"co2\":1600}}")), NODE)).containsExactly("high");
        assertThat(ports(List.of(h.sendTo(NODE, "{\"payload\":{\"co2\":1200}}")), NODE)).containsExactly("warn");
        assertThat(ports(List.of(h.sendTo(NODE, "{\"payload\":{\"co2\":400}}")), NODE)).containsExactly("listed");
        assertThat(ports(List.of(h.sendTo(NODE, "{\"payload\":{\"co2\":700}}")), NODE)).containsExactly("any");
    }

    @Test
    @DisplayName("[FLW-02] TC-FLW-048 rename temperature→temp, pick [temp, deviceId], set unit=C → 기대 JSON과 정확히 일치, 없는 경로 rename 무시")
    void mapRules() {
        FlowTestHarness h = FlowTestHarness.node("transform.map", """
                {"rules":[{"op":"rename","path":"payload.temperature","value":"temp"},{"op":"rename","path":"payload.nothing","value":"x"},
                          {"op":"pick","paths":["temp","deviceId"]},{"op":"set","path":"unit","value":"C"}]}""");

        ExecutionReport r = h.sendTo(NODE, "{\"messageId\":\"m-1\",\"deviceId\":7,\"payload\":{\"temperature\":24.5,\"humidity\":40}}");

        assertThat(outputs(List.of(r), NODE)).singleElement().satisfies(o ->
                assertThat(o.payload().toString()).isEqualTo("{\"messageId\":\"m-1\",\"temp\":24.5,\"deviceId\":7,\"unit\":\"C\"}"));
    }

    @Test
    @DisplayName("[FLW-02] map delete·중첩 set, 입력 메시지는 바뀌지 않는다")
    void mapDelete() {
        FlowTestHarness h = FlowTestHarness.node("transform.map", """
                {"rules":[{"op":"delete","path":"payload.humidity"},{"op":"set","path":"meta.source.kind","value":"flow"}]}""");

        ExecutionReport r = h.sendTo(NODE, "{\"payload\":{\"temperature\":24.5,\"humidity\":40}}");

        assertThat(outputs(List.of(r), NODE).getFirst().payload().toString())
                .isEqualTo("{\"payload\":{\"temperature\":24.5},\"meta\":{\"source\":{\"kind\":\"flow\"}}}");
    }
}
