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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * {@code condition.rateOfChange}(변화율, RUL-01.04·TC-RUL-015): 측정 시각 기준 {@code window} 안에서 가장 이른 값과 지금 값의 차이가
 * {@code delta} 이상인지 본다(방향 up: 올라감, down: 내려감, any: 어느 쪽이든). 대상 키마다 창 안 표본을 상태에 둔다(최대 1,000개).
 * {@code emit}이 {@code change}(규칙 컴파일 기본)이면 판정이 바뀔 때만 true·false를 내고, {@code always}(기본)면 메시지마다 낸다. 내보내는
 * 메시지에 {@code rateOfChange:{from, to, delta, window}}를 더한다. 창 안 표본이 하나뿐이면 판정을 미룬다.
 */
public class RateOfChangeConditionNodeType implements NodeType {

    public static final String TYPE = "condition.rateOfChange";
    static final int MAX_SAMPLES = 1000;
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        String metric = Jsons.text(config, "metric");
        if (metric == null || metric.isBlank()) {
            throw new NodeConfigException("config.metric", "측정 항목(metric)이 필요합니다");
        }
        Duration window = Durations.parse(Jsons.text(config, "window"), "config.window");
        if (window.isZero() || window.isNegative() || window.compareTo(Duration.ofHours(24)) > 0) {
            throw new NodeConfigException("config.window", "창은 0초 초과 24시간 이하입니다");
        }
        Double delta = Jsons.number(config, "delta");
        if (delta == null || delta <= 0) {
            throw new NodeConfigException("config.delta", "변화량(delta)은 0보다 커야 합니다");
        }
        String direction = Jsons.text(config, "direction") == null ? "any" : Jsons.text(config, "direction").toLowerCase(Locale.ROOT);
        if (!List.of("up", "down", "any").contains(direction)) {
            throw new NodeConfigException("config.direction", "방향은 up, down, any 중 하나입니다: " + direction);
        }
        String emit = Jsons.text(config, "emit") == null ? "always" : Jsons.text(config, "emit");
        if (!emit.equals("always") && !emit.equals("change")) {
            throw new NodeConfigException("config.emit", "emit은 always 또는 change입니다: " + emit);
        }
        return new Compiled(metric, window, delta, direction, emit.equals("change"));
    }

    record Compiled(String metric, Duration window, double delta, String direction, boolean onChange) implements CompiledNode {

        @Override
        public List<String> outputs() {
            return List.of("true", "false");
        }

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            JsonNode raw = Messages.value(message, metric);
            if (raw.isMissingNode() || raw.isNull()) {
                return;
            }
            if (!raw.isNumber()) {
                ctx.fail(message, "TYPE_MISMATCH", "숫자가 아닌 값입니다: " + raw);
                return;
            }
            double v = raw.doubleValue();
            Instant at = Messages.measuredAt(message, ctx.now());
            String key = message.targetKey();
            JsonNode state = ctx.state(key);
            List<double[]> samples = new ArrayList<>();
            if (state != null) {
                for (JsonNode s : state.path("samples").values()) {
                    samples.add(new double[]{s.path("t").asLong(0), s.path("v").asDouble()});
                }
            }
            samples.add(new double[]{at.toEpochMilli(), v});
            long cutoff = at.toEpochMilli() - window.toMillis();
            samples.removeIf(s -> s[0] < cutoff);
            while (samples.size() > MAX_SAMPLES) {
                samples.removeFirst();
            }
            if (samples.size() < 2) {
                // 창 안 표본이 하나면 판정을 미룬다(TC-FLW-040)
                ObjectNode only = Jsons.object();
                only.put("active", state != null && state.path("active").asBoolean(false));
                only.putArray("samples").addObject().put("t", at.toEpochMilli()).put("v", v);
                ctx.saveState(key, only);
                return;
            }
            double first = samples.stream().min((a, b) -> Double.compare(a[0], b[0])).map(s -> s[1]).orElse(v);
            double change = v - first;
            boolean cond = switch (direction) {
                case "up" -> change >= delta;
                case "down" -> -change >= delta;
                default -> Math.abs(change) >= delta;
            };
            boolean previous = state != null && state.path("active").asBoolean(false);
            ObjectNode next = Jsons.object();
            next.put("active", cond);
            ArrayNode array = next.putArray("samples");
            samples.forEach(s -> array.addObject().put("t", (long) s[0]).put("v", s[1]));
            ctx.saveState(key, next);
            if (onChange && cond == previous) {
                return;
            }
            ObjectNode body = message.body().deepCopy();
            body.putObject("rateOfChange").put("from", first).put("to", v).put("delta", change).put("window", window.toString());
            ctx.emit(cond ? "true" : "false", message.withBody(body));
        }
    }
}
