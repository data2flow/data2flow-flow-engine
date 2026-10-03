package net.java21.data2flow.flow.node.service;

import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.flow.plan.domain.CompileContext;
import net.java21.data2flow.flow.plan.domain.CompiledNode;
import net.java21.data2flow.flow.plan.domain.DebugSample;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.NodeConfigException;
import net.java21.data2flow.flow.plan.domain.NodeContext;
import net.java21.data2flow.flow.plan.domain.NodeType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * {@code debug.log}(FLW-02 디버그, TC-FLW-059). 지나가는 메시지(또는 {@code fields}로 고른 필드)를 라이브 뷰 샘플로
 * {@code data2flow.debug}(라우팅 키 {@code flow.{flowId}})에 내고 메시지는 그대로 {@code out}으로 넘긴다. 노드당 초당 50건(BR-FLW-12),
 * 손실 허용. 노드를 끄면({@code disabled}) 실행 계획에 들어가지 않아 발행 0건이다.
 */
public class DebugLogNodeType implements NodeType {

    public static final String TYPE = "debug.log";
    private static final Set<String> LEVELS = Set.of("DEBUG", "INFO", "WARN", "ERROR");
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        String level = Jsons.text(config, "level");
        level = level == null ? "INFO" : level.toUpperCase();
        if (!LEVELS.contains(level)) {
            throw new NodeConfigException("config.level", "level은 " + LEVELS + " 중 하나입니다");
        }
        List<String> fields = new ArrayList<>();
        JsonNode f = config.get("fields");
        if (f != null && f.isArray()) {
            f.values().forEach(v -> fields.add(v.asString()));
        }
        return new Compiled(node.id(), level, List.copyOf(fields));
    }

    record Compiled(String nodeId, String level, List<String> fields) implements CompiledNode {

        @Override
        public List<String> outputs() {
            return List.of("out");
        }

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            JsonNode payload;
            if (fields.isEmpty()) {
                payload = message.body();
            } else {
                ObjectNode picked = Jsons.object();
                for (String field : fields) {
                    JsonNode v = Jsons.at(message.body(), Jsons.normalizePath(field));
                    if (!v.isMissingNode()) {
                        picked.set(field, v);
                    }
                }
                payload = picked;
            }
            ObjectNode sample = Jsons.object();
            sample.put("level", level);
            sample.set("data", payload);
            ctx.debug(new DebugSample(nodeId, message.triggerMessageId(), "in", null, sample, true));
            ctx.emit("out", message);
        }
    }
}
