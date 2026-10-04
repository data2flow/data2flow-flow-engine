package net.java21.data2flow.flow.plan.service;

import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.flow.plan.domain.CompileContext;
import net.java21.data2flow.flow.plan.domain.CompiledNode;
import net.java21.data2flow.flow.plan.domain.ExecutionMode;
import net.java21.data2flow.flow.plan.domain.ExecutionPlan;
import net.java21.data2flow.flow.plan.domain.FlowValidationError;
import net.java21.data2flow.flow.plan.domain.NodeConfigException;
import net.java21.data2flow.flow.plan.domain.NodeType;
import net.java21.data2flow.flow.plan.domain.PlanNode;
import net.java21.data2flow.flow.plan.domain.RetryPolicy;
import net.java21.data2flow.flow.plan.domain.TriggerNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * 플로우 정의 → 불변 실행 계획(design/flow-engine-and-live-reload.md §4 ②). 검증 결과는 API-FLW-06·07의 {@code errors[]} 모양
 * ({@code {field, code, message}}, api-rules §5)이고, 하나라도 있으면 계획을 만들지 않는다(이전 버전 유지).
 *
 * <p>검사: 정의 구조({@code structuralErrors}), 알 수 없는 노드 종류, 노드 설정(종류별 compile), 연결선의 출력 포트 존재·포트 타입 호환
 * (BR-FLW-03, FLW-api §5.3), 트리거로 들어가는 연결선, 순환(BR-FLW-04), 트리거 없음(BR-FLW-02), 노드 수 200(BR-FLW-16).
 * 끈 노드({@code disabled})와 그 노드의 연결선은 계획에 넣지 않는다.
 */
public class FlowCompiler {

    public static final int MAX_NODES = 200;
    private final NodeTypeRegistry registry;

    public FlowCompiler(NodeTypeRegistry registry) {
        this.registry = registry;
    }

    /** 컴파일 결과: 계획 또는 오류 목록 */
    public record Result(ExecutionPlan plan, List<FlowValidationError> errors) {

        public boolean ok() {
            return errors.isEmpty();
        }
    }

    public Result compile(UUID flowId, long organizationId, int version, FlowDefinition definition) {
        List<FlowValidationError> errors = new ArrayList<>();
        if (definition == null) {
            return new Result(null, List.of(new FlowValidationError("definition", "INVALID_DEFINITION", "정의가 없습니다")));
        }
        for (String problem : definition.structuralErrors()) {
            errors.add(new FlowValidationError("definition", "INVALID_DEFINITION", problem));
        }
        if (definition.nodes().size() > MAX_NODES) {
            errors.add(new FlowValidationError("nodes", "LIMIT", "플로우당 노드는 " + MAX_NODES + "개까지입니다(BR-FLW-16)"));
        }
        if (!errors.isEmpty()) {
            return new Result(null, errors);
        }
        CompileContext context = new CompileContext(flowId, organizationId, version);
        ExecutionMode mode = ExecutionMode.DEFAULT;
        try {
            mode = ExecutionMode.of(definition.mode());
        } catch (NodeConfigException e) {
            errors.add(new FlowValidationError(e.path(), e.code(), e.getMessage()));
        }
        Map<String, FlowNode> enabled = new LinkedHashMap<>();
        Map<String, NodeType> types = new HashMap<>();
        Map<String, CompiledNode> compiled = new HashMap<>();
        Map<String, RetryPolicy> retries = new HashMap<>();
        for (FlowNode node : definition.nodes()) {
            if (Boolean.TRUE.equals(node.disabled())) {
                continue;
            }
            enabled.put(node.id(), node);
            Optional<NodeType> type = registry.find(node.type());
            if (type.isEmpty()) {
                errors.add(new FlowValidationError(FlowValidationError.node(node.id()) + ".type", "UNKNOWN_NODE_TYPE",
                        "알 수 없는 노드 종류입니다: " + node.type()));
                continue;
            }
            types.put(node.id(), type.get());
            try {
                retries.put(node.id(), RetryPolicy.of(node.retry(), type.get().descriptor().defaults()));
            } catch (NodeConfigException e) {
                errors.add(new FlowValidationError(FlowValidationError.node(node.id()) + "." + e.path(), e.code(), e.getMessage()));
            }
            try {
                compiled.put(node.id(), type.get().compile(node, context));
            } catch (NodeConfigException e) {
                errors.add(new FlowValidationError(FlowValidationError.node(node.id()) + "." + e.path(), e.code(),
                        e.getMessage()));
            } catch (RuntimeException e) {
                errors.add(new FlowValidationError(FlowValidationError.node(node.id()) + ".config", "INVALID_CONFIG",
                        "설정을 해석할 수 없습니다: " + e.getMessage()));
            }
        }
        Map<String, Map<String, List<String>>> wires = new HashMap<>();
        Map<String, Integer> indegree = new HashMap<>();
        enabled.keySet().forEach(id -> indegree.put(id, 0));
        List<FlowDefinition.Wire> list = definition.wires();
        for (int i = 0; i < list.size(); i++) {
            FlowDefinition.Wire w = list.get(i);
            if (!enabled.containsKey(w.from()) || !enabled.containsKey(w.to())) {
                continue; // 끈 노드의 연결선
            }
            CompiledNode from = compiled.get(w.from());
            NodeType fromType = types.get(w.from());
            NodeType toType = types.get(w.to());
            if (from == null || toType == null || fromType == null) {
                continue; // 이미 오류
            }
            String port = w.port() == null || w.port().isBlank() || "out".equals(w.port()) && !from.outputs().contains("out")
                    ? from.outputs().getFirst() : w.port();
            boolean known = from.outputs().contains(port) || FlowNodeType.ERROR_PORT.equals(port);
            if (!known) {
                errors.add(new FlowValidationError(FlowValidationError.wire(i) + ".port", "UNKNOWN_PORT",
                        w.from() + " 노드에 " + port + " 출력 포트가 없습니다(있는 포트: " + from.outputs() + ", error)"));
                continue;
            }
            List<FlowNodeType.Port> inputs = toType.descriptor().inputs();
            if (inputs.isEmpty()) {
                errors.add(new FlowValidationError(FlowValidationError.wire(i), "TYPE_MISMATCH",
                        w.to() + " 노드(" + toType.type() + ")는 입력이 없습니다(트리거)"));
                continue;
            }
            String outType = FlowNodeType.ERROR_PORT.equals(port) ? "error" : portType(fromType.descriptor(), port);
            if (!compatible(outType, inputs.getFirst().type())) {
                errors.add(new FlowValidationError(FlowValidationError.wire(i), "TYPE_MISMATCH",
                        "포트 타입이 맞지 않습니다: " + outType + " → " + inputs.getFirst().type()));
                continue;
            }
            List<String> targets = wires.computeIfAbsent(w.from(), k -> new LinkedHashMap<>())
                    .computeIfAbsent(port, k -> new ArrayList<>());
            if (!targets.contains(w.to())) {
                targets.add(w.to());
                indegree.merge(w.to(), 1, Integer::sum);
            }
        }
        // 위상 정렬(Kahn). 남는 노드가 있으면 순환
        Map<String, Integer> order = new HashMap<>();
        Deque<String> ready = new ArrayDeque<>();
        enabled.keySet().stream().filter(id -> indegree.get(id) == 0).forEach(ready::add);
        Map<String, Integer> remaining = new HashMap<>(indegree);
        while (!ready.isEmpty()) {
            String id = ready.poll();
            order.put(id, order.size());
            for (List<String> targets : wires.getOrDefault(id, Map.of()).values()) {
                for (String t : targets) {
                    if (remaining.merge(t, -1, Integer::sum) == 0) {
                        ready.add(t);
                    }
                }
            }
        }
        if (order.size() < enabled.size()) {
            enabled.keySet().stream().filter(id -> !order.containsKey(id)).sorted().forEach(id -> errors.add(
                    new FlowValidationError(FlowValidationError.node(id), "CYCLE", "순환이 있습니다. 반복은 타이머·대기 노드로 표현합니다(BR-FLW-04)")));
        }
        List<PlanNode> triggers = new ArrayList<>();
        Map<String, PlanNode> nodes = new HashMap<>();
        for (FlowNode node : enabled.values()) {
            CompiledNode c = compiled.get(node.id());
            if (c == null || !order.containsKey(node.id())) {
                continue;
            }
            Map<String, List<String>> w = new HashMap<>();
            wires.getOrDefault(node.id(), Map.of()).forEach((p, t) -> w.put(p, List.copyOf(t)));
            NodeType type = types.get(node.id());
            PlanNode planNode = new PlanNode(node, type, c, order.get(node.id()), Map.copyOf(w), type.stateConfig(node),
                    retries.getOrDefault(node.id(), RetryPolicy.NONE));
            nodes.put(node.id(), planNode);
            if (c instanceof TriggerNode) {
                triggers.add(planNode);
            }
        }
        if (errors.isEmpty() && triggers.isEmpty()) {
            errors.add(new FlowValidationError("nodes", "NO_TRIGGER", "트리거 노드가 1개 이상 있어야 합니다(BR-FLW-02)"));
        }
        if (!errors.isEmpty()) {
            return new Result(null, List.copyOf(errors));
        }
        return new Result(new ExecutionPlan(flowId, organizationId, version, nodes, triggers, mode), List.of());
    }

    private static String portType(FlowNodeType descriptor, String port) {
        return descriptor.outputs().stream().filter(p -> p.name().equals(port)).map(FlowNodeType.Port::type).findFirst()
                .orElse("message");   // 동적 포트(out2.., 케이스)
    }

    /**
     * 포트 타입 호환(BR-FLW-03, FLW-api §5.3): 같은 타입, 한쪽이 {@code any}, 또는 {@code error} 출력 → {@code message} 입력.
     * M3 노드의 입력은 모두 {@code message}라 error 포트도 어느 노드에나 이을 수 있다.
     */
    public static boolean compatible(String outType, String inType) {
        if (outType == null || inType == null || outType.equals(inType) || "any".equals(outType) || "any".equals(inType)) {
            return true;
        }
        return Set.of("error").contains(outType) && "message".equals(inType);
    }
}
