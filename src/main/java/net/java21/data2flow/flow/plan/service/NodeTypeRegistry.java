package net.java21.data2flow.flow.plan.service;

import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.flow.plan.domain.NodeType;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 등록된 노드 종류(카탈로그, API-FLW-83 → core API-FLW-30). M3: 기본 노드 9종 */
public class NodeTypeRegistry {

    private final Map<String, NodeType> types = new LinkedHashMap<>();

    public NodeTypeRegistry(Collection<? extends NodeType> nodeTypes) {
        for (NodeType t : nodeTypes) {
            if (types.putIfAbsent(t.type(), t) != null) {
                throw new IllegalStateException("노드 종류가 두 번 등록되었습니다: " + t.type());
            }
        }
    }

    public Optional<NodeType> find(String type) {
        return Optional.ofNullable(types.get(type));
    }

    public List<NodeType> all() {
        return List.copyOf(types.values());
    }

    /** 카탈로그 항목 목록(type 순서) */
    public List<FlowNodeType> catalog() {
        return types.values().stream().map(NodeType::descriptor).toList();
    }
}
