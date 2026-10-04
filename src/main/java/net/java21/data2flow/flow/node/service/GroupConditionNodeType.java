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
import tools.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * {@code condition.group}(복합 조건, RUL-01.06·TC-RUL-017): 여러 측정 항목 조건을 AND·OR로 묶는다. 측정 항목이 다른 메시지로 따로 와도 되도록
 * 대상 키마다 항목별 마지막 값을 상태에 두고, 메시지마다 마지막 값들로 판정한다. 값을 아직 모르는 항목은 거짓이다. {@code emit}이
 * {@code change}(규칙 컴파일 기본)이면 판정이 바뀔 때만 내보낸다.
 *
 * <p>항목: {@code {metric, op(> >= < <= == != outside inside), value | range[min,max]}}, 1~10개. 그룹 안 지속 시간·연속 횟수는 이 노드에서 보지
 * 않는다(규칙 컴파일러가 거부한다).
 */
public class GroupConditionNodeType implements NodeType {

    public static final String TYPE = "condition.group";
    static final int MAX_ITEMS = 10;
    private static final Set<String> OPS = Set.of(">", ">=", "<", "<=", "==", "!=", "outside", "inside");
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        String op = Jsons.text(config, "op") == null ? "AND" : Jsons.text(config, "op").toUpperCase(Locale.ROOT);
        if (!op.equals("AND") && !op.equals("OR")) {
            throw new NodeConfigException("config.op", "묶음 연산은 AND 또는 OR입니다: " + op);
        }
        JsonNode items = config.get("items");
        if (items == null || !items.isArray() || items.isEmpty() || items.size() > MAX_ITEMS) {
            throw new NodeConfigException("config.items", "조건은 1~" + MAX_ITEMS + "개입니다");
        }
        List<Item> list = new ArrayList<>();
        int i = 0;
        for (JsonNode item : items.values()) {
            String path = "config.items[" + i + "]";
            String metric = Jsons.text(item, "metric");
            String itemOp = Jsons.text(item, "op");
            if (metric == null || metric.isBlank()) {
                throw new NodeConfigException(path + ".metric", "측정 항목이 필요합니다");
            }
            if (itemOp == null || !OPS.contains(itemOp)) {
                throw new NodeConfigException(path + ".op", "비교 연산은 " + OPS + " 중 하나입니다: " + itemOp);
            }
            Double value = Jsons.number(item, "value");
            double min = 0;
            double max = 0;
            if (itemOp.equals("outside") || itemOp.equals("inside")) {
                JsonNode range = item.get("range");
                if (range == null || !range.isArray() || range.size() != 2 || Jsons.number(range.get(0)) == null
                        || Jsons.number(range.get(1)) == null) {
                    throw new NodeConfigException(path + ".range", "outside·inside는 range [min, max]가 필요합니다");
                }
                min = Jsons.number(range.get(0));
                max = Jsons.number(range.get(1));
            } else if (value == null) {
                throw new NodeConfigException(path + ".value", "기준값(value)이 필요합니다");
            }
            list.add(new Item(metric, itemOp, value == null ? 0 : value, min, max));
            i++;
        }
        String emit = Jsons.text(config, "emit") == null ? "always" : Jsons.text(config, "emit");
        return new Compiled(op.equals("AND"), List.copyOf(list), "change".equals(emit));
    }

    record Item(String metric, String op, double value, double min, double max) {
        boolean test(double v) {
            return switch (op) {
                case ">" -> v > value;
                case ">=" -> v >= value;
                case "<" -> v < value;
                case "<=" -> v <= value;
                case "==" -> v == value;
                case "!=" -> v != value;
                case "outside" -> v < min || v > max;
                default -> v >= min && v <= max;
            };
        }
    }

    record Compiled(boolean and, List<Item> items, boolean onChange) implements CompiledNode {

        @Override
        public List<String> outputs() {
            return List.of("true", "false");
        }

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            String key = message.targetKey();
            JsonNode state = ctx.state(key);
            ObjectNode next = state instanceof ObjectNode o ? o.deepCopy() : Jsons.object();
            ObjectNode values = next.has("values") ? (ObjectNode) next.get("values") : next.putObject("values");
            boolean any = false;
            for (Item item : items) {
                JsonNode v = Messages.value(message, item.metric());
                if (v.isNumber()) {
                    values.put(item.metric(), v.doubleValue());
                    any = true;
                }
            }
            if (!any) {
                return;
            }
            boolean result = and;
            for (Item item : items) {
                JsonNode v = values.get(item.metric());
                boolean t = v != null && v.isNumber() && item.test(v.doubleValue());
                result = and ? result && t : result || t;
            }
            boolean previous = next.path("active").asBoolean(false);
            next.put("active", result);
            ctx.saveState(key, next);
            if (onChange && previous == result) {
                return;
            }
            ctx.emit(result ? "true" : "false", message);
        }
    }
}
