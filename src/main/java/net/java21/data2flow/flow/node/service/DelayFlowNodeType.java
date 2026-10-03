package net.java21.data2flow.flow.node.service;

import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.flow.plan.domain.CompileContext;
import net.java21.data2flow.flow.plan.domain.CompiledNode;
import net.java21.data2flow.flow.plan.domain.Durations;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.NodeConfigException;
import net.java21.data2flow.flow.plan.domain.NodeContext;
import net.java21.data2flow.flow.plan.domain.NodeType;
import net.java21.data2flow.flow.plan.domain.TimerFire;
import net.java21.data2flow.flow.plan.domain.TimerKind;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.List;

/**
 * {@code flow.delay}(FLW-02 지연, TC-FLW-049). 스레드를 붙잡지 않고 지속 타이머({@code flow_timers} DELAY, 만기 = 처리 시각 + duration)를
 * 만들고, 만기가 되면 받은 메시지를 그대로 {@code out}으로 내보낸다. 엔진이 재시작되거나 다른 인스턴스로 넘어가도 한 번만 나간다.
 */
public class DelayFlowNodeType implements NodeType {

    public static final String TYPE = "flow.delay";
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        Duration duration = Durations.parse(Jsons.text(node.config(), "duration"), "config.duration");
        if (duration.compareTo(Duration.ofSeconds(1)) < 0 || duration.compareTo(Duration.ofHours(24)) > 0) {
            throw new NodeConfigException("config.duration", "지연 시간은 1초~24시간입니다");
        }
        return new Compiled(duration);
    }

    record Compiled(Duration duration) implements CompiledNode {

        @Override
        public List<String> outputs() {
            return List.of("out");
        }

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            ObjectNode context = Jsons.object();
            context.put("triggerMessageId", message.triggerMessageId());
            context.set("message", message.body());
            ctx.scheduleTimer(TimerKind.DELAY, message.targetKey(), ctx.now().plus(duration), context);
        }

        @Override
        public void onTimer(TimerFire fire, NodeContext ctx) {
            JsonNode body = fire.context().get("message");
            String messageId = fire.context().path("triggerMessageId").asString(null);
            if (body instanceof ObjectNode o && messageId != null) {
                ctx.emit("out", new FlowMessage(o.deepCopy(), messageId, fire.targetKey()));
            }
        }
    }
}
