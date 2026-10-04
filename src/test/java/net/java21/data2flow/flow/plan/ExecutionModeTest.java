package net.java21.data2flow.flow.plan;

import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.flow.plan.domain.ExecutionMode;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.NodeConfigException;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 실행 모드(FLW-05.07, BR-FLW-18) */
class ExecutionModeTest {

    @ParameterizedTest(name = "{0}/{1}/{2}")
    @CsvSource(value = {"queued,deviceId,10,QUEUED,DEVICE,10", "single,deviceId,,SINGLE,DEVICE,10", "restart,spaceId,,RESTART,SPACE,10",
            "parallel,none,3,PARALLEL,NONE,3", ",,,QUEUED,DEVICE,10"})
    @DisplayName("[FLW-05.07] TC-FLW-124 BR-FLW-18 실행 모드 기본은 기기 단위 queued, single·restart·parallel(max) 해석")
    void parse(String concurrency, String keyBy, Integer max, ExecutionMode.Concurrency c, ExecutionMode.KeyBy k, int expectedMax) {
        ExecutionMode m = ExecutionMode.of(new FlowDefinition.Mode(concurrency, keyBy, max));
        assertThat(m.concurrency()).isEqualTo(c);
        assertThat(m.keyBy()).isEqualTo(k);
        assertThat(m.max()).isEqualTo(expectedMax);
        assertThat(ExecutionMode.of(null)).isEqualTo(ExecutionMode.DEFAULT);
    }

    @Test
    @DisplayName("[FLW-05.07] 실행 키: 기기·공간·플로우 전체, 모르는 모드·키·범위 밖 max는 컴파일 오류(mode.*)")
    void runKeyAndErrors() {
        FlowMessage m = new FlowMessage(Jsons.object().put("deviceId", 15).put("spaceId", 31), "m", "space:31");
        assertThat(ExecutionMode.DEFAULT.runKey(m)).isEqualTo("device:15");
        assertThat(ExecutionMode.of(new FlowDefinition.Mode("queued", "spaceId", null)).runKey(m)).isEqualTo("space:31");
        assertThat(ExecutionMode.of(new FlowDefinition.Mode("queued", "none", null)).runKey(m)).isEqualTo("*");
        assertThat(ExecutionMode.DEFAULT.runKey(new FlowMessage(Jsons.object(), "m", "k"))).isEqualTo("k");
        assertThatThrownBy(() -> ExecutionMode.of(new FlowDefinition.Mode("burst", null, null))).isInstanceOf(NodeConfigException.class);
        assertThatThrownBy(() -> ExecutionMode.of(new FlowDefinition.Mode(null, "user", null))).isInstanceOf(NodeConfigException.class);
        assertThatThrownBy(() -> ExecutionMode.of(new FlowDefinition.Mode("parallel", null, 0))).isInstanceOf(NodeConfigException.class);

        FlowDefinition bad = FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","mode":{"concurrency":"burst"},"nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}}],"wires":[]}""");
        assertThat(new FlowCompiler(FlowTestHarness.registry(SpaceDirectory.NONE)).compile(FlowFixtures.FLOW, 1, 1, bad).errors())
                .singleElement().satisfies(e -> assertThat(e.field()).isEqualTo("mode.concurrency"));
    }
}
