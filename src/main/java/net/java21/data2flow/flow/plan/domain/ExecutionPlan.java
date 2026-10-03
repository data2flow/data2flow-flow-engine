package net.java21.data2flow.flow.plan.domain;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 불변 실행 계획(design/flow-engine-and-live-reload.md §1 ①, FLW-06.02의 바탕). 플로우 버전 하나를 컴파일한 결과이고, 메시지는 처리를
 * 시작할 때 읽은 계획 하나로만 끝까지 처리된다(BR-FLW-06). 새 버전은 옆에 완성한 뒤 참조를 한 번에 바꾸고, 이전 계획은 처리 중인 메시지가
 * 모두 끝나면({@link #release()}로 참조 수 0) 닫힌다.
 *
 * @param flowId         플로우
 * @param organizationId 조직
 * @param version        버전
 * @param nodes          노드 ID → 노드
 * @param triggers       텔레메트리 트리거 노드
 */
public record ExecutionPlan(UUID flowId, long organizationId, int version, Map<String, PlanNode> nodes,
                            List<PlanNode> triggers, AtomicInteger references) {

    public ExecutionPlan(UUID flowId, long organizationId, int version, Map<String, PlanNode> nodes, List<PlanNode> triggers) {
        this(flowId, organizationId, version, Map.copyOf(nodes), List.copyOf(triggers), new AtomicInteger());
    }

    public PlanNode node(String nodeId) {
        return nodes.get(nodeId);
    }

    /** 위상 순서대로 정렬한 노드 */
    public List<PlanNode> ordered() {
        return nodes.values().stream().sorted(Comparator.comparingInt(PlanNode::order)).toList();
    }

    public Collection<String> nodeIds() {
        return nodes.keySet();
    }

    /** 메시지 처리 시작(참조 수 +1) */
    public ExecutionPlan acquire() {
        references.incrementAndGet();
        return this;
    }

    /** 메시지 처리 끝(참조 수 −1) */
    public void release() {
        references.decrementAndGet();
    }

    /** 처리 중인 메시지가 없으면 true(드레인 완료) */
    public boolean drained() {
        return references.get() <= 0;
    }
}
