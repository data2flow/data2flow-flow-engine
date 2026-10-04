package net.java21.data2flow.flow.plan;

import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.flow.StatePolicy;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.plan.domain.ExecutionPlan;
import net.java21.data2flow.flow.plan.service.StatePolicyResolver;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** 노드 상태 이어받기 정책 결정표(FLW-06.03, BR-FLW-07) */
class StatePolicyResolverTest {

    private static FlowDefinition def(String threshold, String aggregate, String extra) {
        return FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-agg00001","type":"transform.aggregate","config":%s},
                  {"id":"n-thr00001","type":"condition.threshold","config":%s}%s],
                 "wires":[{"from":"n-trg00001","to":"n-agg00001"},{"from":"n-agg00001","to":"n-thr00001"}]}"""
                .formatted(aggregate, threshold, extra));
    }

    private static ExecutionPlan plan(FlowDefinition d) {
        return FlowTestHarness.of(d, SpaceDirectory.NONE, 1).plan();
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
            "threshold.value 27→28 KEEP|{\"metric\":\"temperature\",\"op\":\">\",\"value\":28,\"for\":\"PT5M\"}|{\"window\":\"PT10M\",\"fn\":\"avg\"}|n-thr00001|KEEP",
            "threshold.metric temperature→co2 RESET|{\"metric\":\"co2\",\"op\":\">\",\"value\":27,\"for\":\"PT5M\"}|{\"window\":\"PT10M\",\"fn\":\"avg\"}|n-thr00001|RESET",
            "threshold.for 5m→10m KEEP|{\"metric\":\"temperature\",\"op\":\">\",\"value\":27,\"for\":\"PT10M\"}|{\"window\":\"PT10M\",\"fn\":\"avg\"}|n-thr00001|KEEP",
            "aggregate.window 10m→15m MIGRATE|{\"metric\":\"temperature\",\"op\":\">\",\"value\":27,\"for\":\"PT5M\"}|{\"window\":\"PT15M\",\"fn\":\"avg\"}|n-agg00001|MIGRATE",
            "aggregate.fn avg→max KEEP|{\"metric\":\"temperature\",\"op\":\">\",\"value\":27,\"for\":\"PT5M\"}|{\"window\":\"PT10M\",\"fn\":\"max\"}|n-agg00001|KEEP",
            "aggregate.groupBy device→space RESET|{\"metric\":\"temperature\",\"op\":\">\",\"value\":27,\"for\":\"PT5M\"}|{\"window\":\"PT10M\",\"fn\":\"avg\",\"groupBy\":\"space\"}|n-agg00001|RESET"})
    @DisplayName("[FLW-06.03] TC-FLW-142 노드 종류별 설정 변경 → 상태 정책 결정표")
    void policyTable(String name, String threshold, String aggregate, String node, StatePolicy expected) {
        ExecutionPlan before = plan(def("{\"metric\":\"temperature\",\"op\":\">\",\"value\":27,\"for\":\"PT5M\"}",
                "{\"window\":\"PT10M\",\"fn\":\"avg\"}", ""));
        ExecutionPlan after = plan(def(threshold, aggregate, ""));

        StatePolicyResolver.Summary summary = StatePolicyResolver.diff(before, after);

        assertThat(summary.changed()).anySatisfy(c -> {
            assertThat(c.nodeId()).isEqualTo(node);
            assertThat(c.statePolicy()).isEqualTo(expected);
        });
    }

    @Test
    @DisplayName("[FLW-06.03] TC-FLW-140 BR-FLW-07: 바뀌지 않은 노드는 요약에 없고, 추가·삭제 노드를 따로 보이며, 노드 ID가 바뀌면 새 노드(삭제+추가), 종류가 바뀌면 RESET")
    void addedRemovedAndTypeChange() {
        ExecutionPlan before = plan(def("{\"metric\":\"temperature\",\"op\":\">\",\"value\":27}", "{\"window\":\"PT10M\",\"fn\":\"avg\"}",
                ",{\"id\":\"n-dbg00001\",\"type\":\"debug.log\",\"config\":{}}"));
        ExecutionPlan after = plan(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-agg00001","type":"transform.map","config":{"rules":[{"op":"set","path":"a","value":1}]}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"temperature","op":">","value":27}},
                  {"id":"n-dbg00002","type":"debug.log","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-agg00001"},{"from":"n-agg00001","to":"n-thr00001"}]}"""));

        StatePolicyResolver.Summary s = StatePolicyResolver.diff(before, after);

        assertThat(s.added()).containsExactly("n-dbg00002");
        assertThat(s.removed()).containsExactly("n-dbg00001");
        assertThat(s.changed()).containsExactly(new StatePolicyResolver.Changed("n-agg00001", StatePolicy.RESET));
        assertThat(s.reset()).containsExactly("n-agg00001");
        assertThat(StatePolicyResolver.diff(null, after).added()).hasSize(4);
    }
}
