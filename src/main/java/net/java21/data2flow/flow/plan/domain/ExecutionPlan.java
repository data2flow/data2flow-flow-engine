package net.java21.data2flow.flow.plan.domain;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 불변 실행 계획(design/flow-engine-and-live-reload.md §1 ①, FLW-06.02). 플로우 버전 하나를 컴파일한 결과이고, 메시지는 처리를
 * 시작할 때 읽은 계획 하나로만 끝까지 처리된다(BR-FLW-06). 새 버전은 옆에 완성한 뒤 참조를 한 번에 바꾸고, 이전 계획은 처리 중인 메시지가
 * 모두 끝나면 닫힌다(참조 카운트 드레인, §4 ⑥).
 *
 * <p>참조 수 {@code references}: 0 이상이면 열린 계획(처리 중인 메시지 수), −1이면 닫힌 계획이다. {@link #tryAcquire()}는 닫힌 계획을
 * 잡지 못하고, {@link #closeIfDrained()}는 참조 수가 0일 때만 −1로 바꾼다(CAS). 그래서 "교체 직후 이전 계획을 잡으려는 메시지"와 "드레인이
 * 끝난 계획을 닫는 정리"가 겹쳐도 닫힌 계획으로 처리되는 메시지는 없다. 잡지 못한 메시지는 참조를 다시 읽어 새 계획을 쓴다.
 *
 * @param flowId         플로우
 * @param organizationId 조직
 * @param version        버전
 * @param nodes          노드 ID → 노드
 * @param triggers       텔레메트리 트리거 노드
 * @param mode           실행 모드(FLW-05.07)
 * @param references     참조 수(−1 = 닫힘)
 */
public record ExecutionPlan(UUID flowId, long organizationId, int version, Map<String, PlanNode> nodes,
                            List<PlanNode> triggers, ExecutionMode mode, AtomicInteger references) {

    public ExecutionPlan(UUID flowId, long organizationId, int version, Map<String, PlanNode> nodes, List<PlanNode> triggers) {
        this(flowId, organizationId, version, nodes, triggers, ExecutionMode.DEFAULT);
    }

    public ExecutionPlan(UUID flowId, long organizationId, int version, Map<String, PlanNode> nodes, List<PlanNode> triggers,
                         ExecutionMode mode) {
        this(flowId, organizationId, version, Map.copyOf(nodes), List.copyOf(triggers),
                mode == null ? ExecutionMode.DEFAULT : mode, new AtomicInteger());
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

    /** 메시지 처리 시작(참조 수 +1). 이미 닫힌 계획이면 false */
    public boolean tryAcquire() {
        while (true) {
            int current = references.get();
            if (current < 0) {
                return false;
            }
            if (references.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /** 메시지 처리 시작. 닫힌 계획이면 예외(시험·드라이런처럼 교체가 없는 곳에서 쓴다) */
    public ExecutionPlan acquire() {
        if (!tryAcquire()) {
            throw new IllegalStateException("닫힌 실행 계획입니다: " + flowId + " v" + version);
        }
        return this;
    }

    /** 메시지 처리 끝(참조 수 −1) */
    public void release() {
        references.decrementAndGet();
    }

    /** 처리 중인 메시지가 없으면 true(드레인 완료 또는 닫힘) */
    public boolean drained() {
        return references.get() <= 0;
    }

    /** 처리 중인 메시지 수(닫혔으면 0) */
    public int inFlight() {
        return Math.max(0, references.get());
    }

    /** 처리 중인 메시지가 없으면 닫는다(이후 {@link #tryAcquire()} 실패). 닫혔으면 true */
    public boolean closeIfDrained() {
        if (references.compareAndSet(0, -1)) {
            for (PlanNode node : nodes.values()) {
                try {
                    node.compiled().close();
                } catch (RuntimeException ignored) {
                    // 정리 실패는 무시(다음 GC가 거둠)
                }
            }
            return true;
        }
        return references.get() < 0;
    }

    public boolean closed() {
        return references.get() < 0;
    }
}
