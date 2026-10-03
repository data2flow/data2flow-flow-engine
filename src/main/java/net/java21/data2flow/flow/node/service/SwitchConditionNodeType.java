package net.java21.data2flow.flow.node.service;

import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.flow.plan.domain.CompileContext;
import net.java21.data2flow.flow.plan.domain.CompiledNode;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.NodeConfigException;
import net.java21.data2flow.flow.plan.domain.NodeContext;
import net.java21.data2flow.flow.plan.domain.NodeType;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@code condition.switch}(FLW-02 스위치, FLW-01.03, TC-FLW-044). {@code expression}(JSONPath 부분집합 {@code $.a.b[0]})으로 값을 꺼내
 * 케이스를 위에서부터 비교하고 <b>처음 맞는 케이스 포트 하나</b>로만 내보낸다. 맞는 것이 없거나 경로가 없으면 {@code default}.
 * 연산: ==, !=(숫자·문자열·불린), >, >=, <, <=(숫자), in(배열 값 중 하나), exists(값이 있음).
 */
public class SwitchConditionNodeType implements NodeType {

    public static final String TYPE = "condition.switch";
    private static final Set<String> OPS = Set.of("==", "!=", ">", ">=", "<", "<=", "in", "exists");
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        String expression = Jsons.text(config, "expression");
        if (expression == null || expression.isBlank()) {
            throw new NodeConfigException("config.expression", "값 경로(expression)가 필요합니다");
        }
        JsonNode cases = config.get("cases");
        if (cases == null || !cases.isArray() || cases.isEmpty()) {
            throw new NodeConfigException("config.cases", "케이스가 1개 이상 필요합니다");
        }
        List<Case> list = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (int i = 0; i < cases.size(); i++) {
            JsonNode c = cases.get(i);
            String name = Jsons.text(c, "name");
            String op = Jsons.text(c, "op");
            if (name == null || !name.matches("[A-Za-z0-9_-]{1,32}") || "default".equals(name) || "error".equals(name)
                    || !names.add(name)) {
                throw new NodeConfigException("config.cases[" + i + "].name", "케이스 이름은 겹치지 않는 영숫자·_·- 1~32자(default·error 제외)입니다");
            }
            if (op == null || !OPS.contains(op)) {
                throw new NodeConfigException("config.cases[" + i + "].op", "연산은 " + OPS + " 중 하나입니다");
            }
            if ("in".equals(op) && !c.path("value").isArray()) {
                throw new NodeConfigException("config.cases[" + i + "].value", "in은 배열 값이 필요합니다");
            }
            list.add(new Case(name, op, c.get("value")));
        }
        List<String> outputs = new ArrayList<>(list.stream().map(Case::name).toList());
        outputs.add("default");
        return new Compiled(Jsons.normalizePath(expression), List.copyOf(list), List.copyOf(outputs));
    }

    record Case(String name, String op, JsonNode value) {

        boolean matches(JsonNode actual) {
            boolean present = actual != null && !actual.isMissingNode() && !actual.isNull();
            return switch (op) {
                case "exists" -> present;
                case "in" -> present && value.values().stream().anyMatch(v -> same(actual, v));
                case "==" -> present && same(actual, value);
                case "!=" -> !present || !same(actual, value);
                default -> {
                    Double a = present ? Jsons.number(actual) : null;
                    Double b = Jsons.number(value);
                    if (a == null || b == null) {
                        yield false;
                    }
                    yield switch (op) {
                        case ">" -> a > b;
                        case ">=" -> a >= b;
                        case "<" -> a < b;
                        default -> a <= b;
                    };
                }
            };
        }

        private static boolean same(JsonNode a, JsonNode b) {
            if (b == null) {
                return false;
            }
            if (a.isNumber() && b.isNumber()) {
                return a.doubleValue() == b.doubleValue();
            }
            return a.equals(b);
        }
    }

    record Compiled(String path, List<Case> cases, List<String> outputs) implements CompiledNode {

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            JsonNode actual = Jsons.at(message.body(), path);
            for (Case c : cases) {
                if (c.matches(actual)) {
                    ctx.emit(c.name(), message);
                    return;
                }
            }
            ctx.emit("default", message);
        }
    }
}
