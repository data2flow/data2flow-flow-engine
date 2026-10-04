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
import java.time.Instant;
import java.util.List;

/**
 * {@code condition.noData}(무수신, RUL-01.05·TC-RUL-016): 대상 키(기기)의 메시지가 {@code window} 동안 오지 않으면 {@code timeout}으로 한 번,
 * 그 뒤 다시 오면 {@code restored}로 한 번 내보낸다. 메시지가 끊겨도 정확한 시각에 판정하도록 지속 타이머(RECHECK)를 쓴다. 메시지마다 타이머를
 * 다시 걸지 않고 대상 키당 타이머 하나를 두며, 만기 때 마지막 수신 시각을 보고 아직이면 다시 건다(DB 쓰기를 줄임).
 *
 * <p>{@code metric}을 주면 그 항목이 들어 있는 메시지만 수신으로 센다. 내보내는 메시지: timeout은 마지막으로 받은 메시지에
 * {@code noData:{since, window}}, restored는 다시 받은 메시지. 포트 {@code restored}는 FLW-api 카탈로그 표에 없는 추가 포트다(규칙 자동 해제용).
 */
public class NoDataConditionNodeType implements NodeType {

    public static final String TYPE = "condition.noData";
    private static final Duration MIN = Duration.ofSeconds(10);
    private static final Duration MAX = Duration.ofDays(7);
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        String w = Jsons.text(config, "window");
        Duration window = Durations.parse(w, "config.window");
        if (window.compareTo(MIN) < 0 || window.compareTo(MAX) > 0) {
            throw new NodeConfigException("config.window", "무수신 기준은 10초~7일입니다: " + w);
        }
        return new Compiled(Jsons.text(config, "metric"), window);
    }

    record Compiled(String metric, Duration window) implements CompiledNode {

        @Override
        public List<String> outputs() {
            return List.of("timeout", "restored");
        }

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            if (metric != null && Messages.value(message, metric).isMissingNode()) {
                return;
            }
            String key = message.targetKey();
            JsonNode state = ctx.state(key);
            boolean timedOut = state != null && state.path("timedOut").asBoolean(false);
            Instant now = ctx.now();
            ObjectNode next = Jsons.object();
            next.put("lastSeen", now.toString());
            next.put("triggerMessageId", message.triggerMessageId());
            next.set("message", message.body());
            long timer = state == null ? 0 : state.path("timerId").asLong(0);
            if (timer == 0 || timedOut) {
                timer = ctx.scheduleTimer(TimerKind.RECHECK, key, now.plus(window), Jsons.object().put("noData", true));
            }
            next.put("timerId", timer);
            next.put("timedOut", false);
            ctx.saveState(key, next);
            if (timedOut) {
                ctx.emit("restored", message);
            }
        }

        @Override
        public void onTimer(TimerFire fire, NodeContext ctx) {
            JsonNode state = ctx.state(fire.targetKey());
            if (state == null || state.path("timerId").asLong(0) != fire.timerId() || state.path("timedOut").asBoolean(false)) {
                return;
            }
            Instant lastSeen = Instant.parse(state.path("lastSeen").asString());
            Instant due = lastSeen.plus(window);
            ObjectNode next = (ObjectNode) state.deepCopy();
            if (due.isAfter(ctx.now())) {
                // 그 사이 메시지가 왔다: 마지막 수신 기준으로 다시 건다
                next.put("timerId", ctx.scheduleTimer(TimerKind.RECHECK, fire.targetKey(), due, Jsons.object().put("noData", true)));
                ctx.saveState(fire.targetKey(), next);
                return;
            }
            next.put("timedOut", true);
            next.remove("timerId");
            ctx.saveState(fire.targetKey(), next);
            JsonNode body = state.get("message");
            ObjectNode out = body instanceof ObjectNode o ? o.deepCopy() : Jsons.object();
            out.putObject("noData").put("since", lastSeen.toString()).put("window", window.toString());
            ctx.emit("timeout", new FlowMessage(out, state.path("triggerMessageId").asString("timer:" + fire.timerId())
                    + ":nodata:" + lastSeen.toEpochMilli(), fire.targetKey()));
        }
    }
}
