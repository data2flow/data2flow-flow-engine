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

/**
 * {@code transform.map}(FLW-02 필드 매핑, "change" 노드, TC-FLW-048). 규칙을 차례로 적용한다. 경로는 메시지 루트 기준 점 경로
 * ({@code payload.temperature}).
 * <ul>
 *   <li>rename {path, value: 새 경로}: 값을 옮긴다. 없는 경로는 무시</li>
 *   <li>pick {paths: [...]}: 그 경로들만 남긴다(+ 엔진 필드 messageId는 항상 남김)</li>
 *   <li>set {path, value}: 값을 넣는다(중간 객체는 만든다)</li>
 *   <li>delete {path}: 지운다</li>
 * </ul>
 */
public class MapTransformNodeType implements NodeType {

    public static final String TYPE = "transform.map";
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode rules = node.config() == null ? null : node.config().get("rules");
        if (rules == null || !rules.isArray() || rules.isEmpty()) {
            throw new NodeConfigException("config.rules", "규칙이 1개 이상 필요합니다");
        }
        List<Rule> list = new ArrayList<>();
        for (int i = 0; i < rules.size(); i++) {
            JsonNode r = rules.get(i);
            String op = Jsons.text(r, "op");
            String path = Jsons.text(r, "path");
            String at = "config.rules[" + i + "]";
            switch (op == null ? "" : op) {
                case "rename" -> {
                    String to = Jsons.text(r, "value");
                    require(path, at + ".path");
                    require(to, at + ".value");
                    list.add(new Rule(op, List.of(path), null, to));
                }
                case "pick" -> {
                    List<String> paths = new ArrayList<>();
                    JsonNode ps = r.get("paths");
                    if (ps != null && ps.isArray()) {
                        ps.values().forEach(p -> paths.add(p.asString()));
                    } else if (r.path("value").isArray()) {
                        r.get("value").values().forEach(p -> paths.add(p.asString()));
                    } else if (path != null) {
                        paths.add(path);
                    }
                    if (paths.isEmpty()) {
                        throw new NodeConfigException(at + ".paths", "pick은 남길 경로가 필요합니다");
                    }
                    list.add(new Rule(op, List.copyOf(paths), null, null));
                }
                case "set" -> {
                    require(path, at + ".path");
                    if (!r.has("value")) {
                        throw new NodeConfigException(at + ".value", "set은 값(value)이 필요합니다");
                    }
                    list.add(new Rule(op, List.of(path), r.get("value"), null));
                }
                case "delete" -> {
                    require(path, at + ".path");
                    list.add(new Rule(op, List.of(path), null, null));
                }
                default -> throw new NodeConfigException(at + ".op", "op는 rename, pick, set, delete 중 하나입니다: " + op);
            }
        }
        return new Compiled(List.copyOf(list));
    }

    private static void require(String value, String path) {
        if (value == null || value.isBlank()) {
            throw new NodeConfigException(path, "경로가 필요합니다");
        }
    }

    record Rule(String op, List<String> paths, JsonNode value, String to) {
    }

    record Compiled(List<Rule> rules) implements CompiledNode {

        @Override
        public List<String> outputs() {
            return List.of("out");
        }

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            ObjectNode body = message.body().deepCopy();
            for (Rule rule : rules) {
                switch (rule.op()) {
                    case "rename" -> {
                        JsonNode v = Jsons.at(body, rule.paths().getFirst());
                        if (!v.isMissingNode()) {
                            remove(body, rule.paths().getFirst());
                            put(body, rule.to(), v);
                        }
                    }
                    case "pick" -> {
                        ObjectNode picked = Jsons.object();
                        if (body.has("messageId")) {
                            picked.set("messageId", body.get("messageId"));
                        }
                        for (String p : rule.paths()) {
                            JsonNode v = Jsons.at(body, p);
                            if (!v.isMissingNode()) {
                                put(picked, p, v);
                            }
                        }
                        body = picked;
                    }
                    case "set" -> put(body, rule.paths().getFirst(), rule.value().deepCopy());
                    default -> remove(body, rule.paths().getFirst());
                }
            }
            ctx.emit("out", message.withBody(body));
        }

        private static void put(ObjectNode root, String path, JsonNode value) {
            String[] parts = path.split("\\.");
            ObjectNode current = root;
            for (int i = 0; i < parts.length - 1; i++) {
                JsonNode child = current.get(parts[i]);
                if (child == null || !child.isObject()) {
                    child = current.putObject(parts[i]);
                }
                current = (ObjectNode) child;
            }
            current.set(parts[parts.length - 1], value);
        }

        private static void remove(ObjectNode root, String path) {
            int dot = path.lastIndexOf('.');
            JsonNode parent = dot < 0 ? root : Jsons.at(root, path.substring(0, dot));
            if (parent instanceof ObjectNode o) {
                o.remove(dot < 0 ? path : path.substring(dot + 1));
            }
        }
    }
}
