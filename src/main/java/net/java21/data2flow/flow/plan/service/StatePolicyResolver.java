package net.java21.data2flow.flow.plan.service;

import net.java21.data2flow.contracts.flow.StatePolicy;
import net.java21.data2flow.flow.plan.domain.ExecutionPlan;
import net.java21.data2flow.flow.plan.domain.PlanNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 새 버전 적용 때 노드별 상태 처리(FLW-06.03, BR-FLW-07, design §4.3)와 적용 전 검증의 변경 요약(API-FLW-06 {@code changeSummary}).
 *
 * <table>
 *   <tr><th>변경</th><th>처리</th></tr>
 *   <tr><td>노드 그대로(같은 ID·종류·설정)</td><td>그대로 씀</td></tr>
 *   <tr><td>설정 변경</td><td>노드 종류의 {@code statePolicy(old, new)}: KEEP·RESET·MIGRATE</td></tr>
 *   <tr><td>노드 종류 변경(같은 ID)</td><td>RESET</td></tr>
 *   <tr><td>노드 추가</td><td>빈 상태</td></tr>
 *   <tr><td>노드 삭제</td><td>24시간 보관(롤백하면 복원)</td></tr>
 * </table>
 */
public final class StatePolicyResolver {

    private StatePolicyResolver() {
    }

    /** 바뀐 노드 하나 */
    public record Changed(String nodeId, StatePolicy statePolicy) {
    }

    /**
     * 변경 요약.
     *
     * @param added   새 노드
     * @param removed 삭제된 노드(상태 24시간 보관)
     * @param changed 설정·종류가 바뀐 노드와 상태 정책
     */
    public record Summary(List<String> added, List<String> removed, List<Changed> changed) {

        /** 정책이 RESET인 노드 */
        public List<String> reset() {
            return changed.stream().filter(c -> c.statePolicy() == StatePolicy.RESET).map(Changed::nodeId).toList();
        }
    }

    public static Summary diff(ExecutionPlan previous, ExecutionPlan next) {
        List<String> added = new ArrayList<>();
        List<String> removed = new ArrayList<>();
        List<Changed> changed = new ArrayList<>();
        for (PlanNode n : next.ordered()) {
            PlanNode old = previous == null ? null : previous.node(n.id());
            if (old == null) {
                added.add(n.id());
                continue;
            }
            boolean sameType = old.definition().type().equals(n.definition().type());
            boolean sameConfig = Objects.equals(old.definition().config(), n.definition().config());
            if (sameType && sameConfig) {
                continue;
            }
            StatePolicy policy = sameType ? n.type().statePolicy(old.stateConfig(), n.stateConfig()) : StatePolicy.RESET;
            changed.add(new Changed(n.id(), policy));
        }
        if (previous != null) {
            previous.ordered().stream().filter(o -> next.node(o.id()) == null).forEach(o -> removed.add(o.id()));
        }
        return new Summary(List.copyOf(added), List.copyOf(removed), List.copyOf(changed));
    }
}
