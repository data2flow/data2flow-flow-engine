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
import java.util.Set;

/**
 * {@code condition.threshold}(FLW-02 조건, FLW-05.02, TC-FLW-039, AT-FLW-24.2·24.3).
 *
 * <ul>
 *   <li><b>상태 없는 판정</b>({@code for}·{@code clear} 둘 다 없음): 메시지마다 참이면 {@code true}, 거짓이면 {@code false} 포트.</li>
 *   <li><b>상태 판정</b>({@code for} 또는 {@code clear}): 대상 키마다 IDLE → PENDING → ACTIVE 상태를 {@code flow_node_state}에 두고,
 *       <b>바뀔 때만</b> 내보낸다. ACTIVE가 되는 순간 {@code true} 1건, 해제되는 순간 {@code false} 1건.</li>
 *   <li><b>지속 시간 {@code for}</b>: 조건이 처음 참이 된 메시지의 측정 시각부터 센다. (a) 그 뒤 참인 메시지의 측정 시각이 시작 +
 *       {@code for} 이상이거나(측정 시각 기준, x60 가속 시뮬레이션도 시뮬레이션 시각으로 맞게 판정), (b) 메시지가 오지 않아도 지속 타이머
 *       ({@code flow_timers} RECHECK, 만기 = 시작 시점의 처리 시각 + {@code for})가 만기가 되면 ACTIVE가 된다. 둘 중 먼저 온 쪽 한 번만
 *       발생한다. 중간에 거짓 메시지가 오면 PENDING을 버리고 타이머를 취소한다.</li>
 *   <li><b>히스테리시스 {@code clear}</b>: ACTIVE는 {@code >}·{@code >=}이면 값 ≤ clear, {@code <}·{@code <=}이면 값 ≥ clear일 때 해제된다
 *       (예: 발생 27, 해제 26이면 26.5는 해제되지 않음). 없으면 조건이 거짓이 될 때 해제.</li>
 *   <li>메시지에 그 측정 항목이 없으면 아무것도 내보내지 않고, 숫자가 아니면 error 포트({@code TYPE_MISMATCH}).</li>
 * </ul>
 * 타이머 발화로 나가는 메시지는 PENDING을 시작한 메시지(이후 마지막 참 메시지로 갱신)이고 원인 메시지 ID도 그 메시지의 것이라, 같은 발생은
 * 어느 인스턴스가 발화해도 같은 행동 멱등 키를 만든다(BR-FLW-13).
 */
public class ThresholdConditionNodeType implements NodeType {

    public static final String TYPE = "condition.threshold";
    private static final Set<String> OPS = Set.of(">", ">=", "<", "<=", "==", "!=", "outside", "inside");
    private static final Duration MAX_FOR = Duration.ofHours(24);
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        String op = Jsons.text(config, "op");
        if (op == null || !OPS.contains(op)) {
            throw new NodeConfigException("config.op", "비교 연산은 " + OPS + " 중 하나입니다: " + op);
        }
        Double value = Jsons.number(config, "value");
        double min = 0;
        double max = 0;
        if ("outside".equals(op) || "inside".equals(op)) {
            JsonNode range = config.get("range");
            if (range == null || !range.isArray() || range.size() != 2 || Jsons.number(range.get(0)) == null
                    || Jsons.number(range.get(1)) == null) {
                throw new NodeConfigException("config.range", "outside·inside는 range [min, max]가 필요합니다");
            }
            min = Jsons.number(range.get(0));
            max = Jsons.number(range.get(1));
            if (min > max) {
                throw new NodeConfigException("config.range", "range는 min ≤ max여야 합니다");
            }
        } else if (value == null) {
            throw new NodeConfigException("config.value", "기준값(value)이 필요합니다");
        }
        Duration forDuration = null;
        String forText = Jsons.text(config, "for");
        if (forText != null && !forText.isBlank()) {
            forDuration = Durations.parse(forText, "config.for");
            if (forDuration.isNegative() || forDuration.compareTo(MAX_FOR) > 0) {
                throw new NodeConfigException("config.for", "지속 시간은 0~24시간입니다: " + forText);
            }
        }
        Double clear = Jsons.number(config, "clear");
        if (config.has("repeat")) {
            throw new NodeConfigException("config.repeat", "repeat는 아직 지원하지 않습니다(M4)");
        }
        return new Compiled(Jsons.text(config, "metric"), op, value == null ? 0 : value, min, max, forDuration, clear);
    }

    /** 상태 단계 */
    enum Phase { IDLE, PENDING, ACTIVE }

    record Compiled(String metric, String op, double value, double min, double max, Duration forDuration, Double clear)
            implements CompiledNode {

        @Override
        public List<String> outputs() {
            return List.of("true", "false");
        }

        boolean stateful() {
            return forDuration != null || clear != null;
        }

        boolean test(double v) {
            return switch (op) {
                case ">" -> v > value;
                case ">=" -> v >= value;
                case "<" -> v < value;
                case "<=" -> v <= value;
                case "==" -> v == value;
                case "!=" -> v != value;
                case "outside" -> v < min || v > max;
                default -> v >= min && v <= max;   // inside
            };
        }

        boolean cleared(double v) {
            if (clear == null) {
                return !test(v);
            }
            return switch (op) {
                case ">", ">=" -> v <= clear;
                case "<", "<=" -> v >= clear;
                default -> !test(v);
            };
        }

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            JsonNode raw = Messages.value(message, metric);
            if (raw.isMissingNode() || raw.isNull() || (raw.isObject() && metric == null)) {
                return; // 이 메시지에는 그 측정 항목이 없다
            }
            if (!raw.isNumber()) {
                ctx.fail(message, "TYPE_MISMATCH", "숫자가 아닌 값입니다: " + raw);
                return;
            }
            double v = raw.doubleValue();
            boolean cond = test(v);
            if (!stateful()) {
                ctx.emit(cond ? "true" : "false", message);
                return;
            }
            String key = message.targetKey();
            JsonNode state = ctx.state(key);
            Phase phase = state == null ? Phase.IDLE : Phase.valueOf(state.path("phase").asString("IDLE"));
            Instant at = Messages.measuredAt(message, ctx.now());
            switch (phase) {
                case IDLE -> {
                    if (!cond) {
                        return;
                    }
                    if (forDuration == null || forDuration.isZero()) {
                        ctx.saveState(key, phaseState(Phase.ACTIVE));
                        ctx.emit("true", message);
                        return;
                    }
                    ObjectNode next = phaseState(Phase.PENDING);
                    next.put("since", at.toString());
                    next.put("pendingMessageId", message.triggerMessageId());
                    next.set("message", message.body());
                    long timer = ctx.scheduleTimer(TimerKind.RECHECK, key, ctx.now().plus(forDuration), timerContext(message));
                    next.put("timerId", timer);
                    ctx.saveState(key, next);
                }
                case PENDING -> {
                    long timer = state.path("timerId").asLong(0);
                    if (!cond) {
                        if (timer > 0) {
                            ctx.cancelTimer(timer);
                        }
                        ctx.saveState(key, phaseState(Phase.IDLE));
                        return;
                    }
                    Instant since = Instant.parse(state.path("since").asString());
                    if (!at.isBefore(since.plus(forDuration))) {
                        if (timer > 0) {
                            ctx.cancelTimer(timer);
                        }
                        ctx.saveState(key, phaseState(Phase.ACTIVE));
                        ctx.emit("true", message);
                        return;
                    }
                    ObjectNode next = (ObjectNode) state.deepCopy();
                    next.set("message", message.body());
                    ctx.saveState(key, next);
                }
                case ACTIVE -> {
                    if (cleared(v)) {
                        ctx.saveState(key, phaseState(Phase.IDLE));
                        ctx.emit("false", message);
                    }
                }
            }
        }

        @Override
        public void onTimer(TimerFire fire, NodeContext ctx) {
            JsonNode state = ctx.state(fire.targetKey());
            if (state == null || !"PENDING".equals(state.path("phase").asString(""))
                    || state.path("timerId").asLong(0) != fire.timerId()) {
                return; // 이미 해제·발생했거나 다른 타이머(오래된 발화)
            }
            JsonNode body = state.get("message");
            String messageId = state.path("pendingMessageId").asString(null);
            if (body == null || !body.isObject() || messageId == null) {
                body = fire.context().get("message");
                messageId = fire.context().path("triggerMessageId").asString(null);
            }
            ctx.saveState(fire.targetKey(), phaseState(Phase.ACTIVE));
            ctx.emit("true", new FlowMessage(((ObjectNode) body).deepCopy(), messageId, fire.targetKey()));
        }

        private static ObjectNode phaseState(Phase phase) {
            ObjectNode s = Jsons.object();
            s.put("phase", phase.name());
            return s;
        }

        private static ObjectNode timerContext(FlowMessage message) {
            ObjectNode c = Jsons.object();
            c.put("triggerMessageId", message.triggerMessageId());
            c.set("message", message.body());
            return c;
        }
    }
}
