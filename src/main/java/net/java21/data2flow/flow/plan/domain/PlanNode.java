package net.java21.data2flow.flow.plan.domain;

import net.java21.data2flow.contracts.flow.FlowNode;

import java.util.List;
import java.util.Map;

/**
 * 실행 계획 안의 노드 하나(불변).
 *
 * @param definition 노드 정의
 * @param type       노드 종류
 * @param compiled   컴파일된 노드
 * @param order      위상 정렬 순서(실행 순서와 상태 잠금 순서. 같은 계획을 쓰는 실행끼리 잠금 순서가 같아 교착이 생기지 않는다)
 * @param wires      출력 포트 → 받는 노드 ID 목록(포트 이름은 기본 포트로 정규화됨)
 */
public record PlanNode(FlowNode definition, NodeType type, CompiledNode compiled, int order,
                       Map<String, List<String>> wires) {

    public String id() {
        return definition.id();
    }

    public List<String> targets(String port) {
        return wires.getOrDefault(port, List.of());
    }

    public boolean hasWires(String port) {
        return !targets(port).isEmpty();
    }
}
