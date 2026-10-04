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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@code transform.aggregate}(FLW-02 집계·창, TC-FLW-047, AT-FLW-24.5). 묶음 키(기기·공간·전체)마다 최근 {@code window}(측정 시각 기준)의
 * 표본을 {@code flow_node_state}에 두고, 입력 하나마다 묶음의 집계값 1건을 내보낸다. 창 경계: 마지막 측정 시각 − window보다 오래된 표본은
 * 버린다(10분 창이면 10분 1초 전 표본 제외). 표본은 묶음당 최대 {@value #MAX_SAMPLES}개(상태 256KB 안).
 *
 * <p>출력 메시지는 입력을 복사하고 {@code payload}를 집계값으로 바꾼 것({@code {temperature: 27.5}})이며 {@code aggregate:{fn, window,
 * count, groupBy}}를 더한다. 대상 키는 묶음 키({@code space:31})로 바뀌어 뒤 노드(임계값)의 상태도 공간 단위가 된다.
 */
public class AggregateTransformNodeType implements NodeType {

    public static final String TYPE = "transform.aggregate";
    static final int MAX_SAMPLES = 2000;
    private static final Set<String> FNS = Set.of("avg", "min", "max", "sum", "count");
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        Duration window = Durations.parse(Jsons.text(config, "window"), "config.window");
        if (window.isZero() || window.isNegative() || window.compareTo(Duration.ofDays(1)) > 0) {
            throw new NodeConfigException("config.window", "창은 0초 초과 24시간 이하입니다");
        }
        String fn = Jsons.text(config, "fn");
        if (fn == null || !FNS.contains(fn)) {
            throw new NodeConfigException("config.fn", "fn은 " + FNS + " 중 하나입니다");
        }
        String groupBy = Jsons.text(config, "groupBy");
        groupBy = groupBy == null ? "device" : groupBy;
        if (!Set.of("device", "space", "all").contains(groupBy)) {
            throw new NodeConfigException("config.groupBy", "groupBy는 device, space, all 중 하나입니다");
        }
        return new Compiled(window, fn, groupBy, Jsons.text(config, "metric"));
    }

    /**
     * MIGRATE(창 길이 변경, FLW-06.03·TC-FLW-141): 표본을 새 창 길이로 자른다(가장 최근 표본 기준). 창이 길어지면 모든 표본을 그대로 둔다.
     */
    @Override
    public JsonNode migrateState(JsonNode oldConfig, JsonNode newConfig, JsonNode state) {
        String windowText = Jsons.text(newConfig, "window");
        if (state == null || windowText == null || !state.path("samples").isArray()) {
            return state;
        }
        Duration window = Durations.parse(windowText, "config.window");
        long latest = 0;
        for (JsonNode s : state.get("samples").values()) {
            latest = Math.max(latest, s.path("t").asLong(0));
        }
        long cutoff = latest - window.toMillis();
        ObjectNode next = Jsons.object();
        ArrayNode array = next.putArray("samples");
        for (JsonNode s : state.get("samples").values()) {
            if (s.path("t").asLong(0) >= cutoff) {
                array.add(s);
            }
        }
        return next;
    }

    record Compiled(Duration window, String fn, String groupBy, String metric) implements CompiledNode {

        @Override
        public List<String> outputs() {
            return List.of("out");
        }

        String groupKey(FlowMessage message) {
            return switch (groupBy) {
                case "space" -> {
                    String space = Jsons.text(message.body(), "spaceId");
                    yield space == null ? message.targetKey() : "space:" + space;
                }
                case "all" -> "all";
                default -> {
                    String device = Jsons.text(message.body(), "deviceId");
                    yield device == null ? message.targetKey() : "device:" + device;
                }
            };
        }

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            Map<String, Double> values = new LinkedHashMap<>();
            JsonNode payload = message.body().path("payload");
            if (metric != null) {
                JsonNode v = Messages.value(message, metric);
                if (v.isNumber()) {
                    values.put(metric, v.doubleValue());
                } else if (!v.isMissingNode() && !v.isNull()) {
                    ctx.fail(message, "TYPE_MISMATCH", "숫자가 아닌 값입니다: " + v);
                    return;
                }
            } else if (payload.isNumber()) {
                values.put("value", payload.doubleValue());
            } else if (payload.isObject()) {
                payload.properties().forEach(e -> {
                    if (e.getValue().isNumber()) {
                        values.put(e.getKey(), e.getValue().doubleValue());
                    }
                });
            }
            if (values.isEmpty()) {
                return;
            }
            String key = groupKey(message);
            Instant at = Messages.measuredAt(message, ctx.now());
            String device = Jsons.text(message.body(), "deviceId");
            JsonNode state = ctx.state(key);
            List<JsonNode> samples = new ArrayList<>();
            Instant latest = at;
            if (state != null && state.path("samples").isArray()) {
                for (JsonNode s : state.get("samples").values()) {
                    samples.add(s);
                    Instant t = Instant.ofEpochMilli(s.path("t").asLong(0));
                    if (t.isAfter(latest)) {
                        latest = t;
                    }
                }
            }
            for (Map.Entry<String, Double> e : values.entrySet()) {
                ObjectNode s = Jsons.object();
                s.put("t", at.toEpochMilli());
                s.put("k", e.getKey());
                s.put("v", e.getValue());
                if (device != null) {
                    s.put("d", device);
                }
                samples.add(s);
            }
            long cutoff = latest.minus(window).toEpochMilli();
            samples.removeIf(s -> s.path("t").asLong(0) < cutoff);
            while (samples.size() > MAX_SAMPLES) {
                samples.removeFirst();
            }
            ObjectNode nextState = Jsons.object();
            ArrayNode array = nextState.putArray("samples");
            samples.forEach(array::add);
            ctx.saveState(key, nextState);

            ObjectNode out = message.body().deepCopy();
            ObjectNode aggregated = Jsons.object();
            int count = 0;
            for (String k : values.keySet()) {
                List<Double> vs = samples.stream().filter(s -> k.equals(s.path("k").asString(""))).map(s -> s.path("v").asDouble())
                        .toList();
                count = Math.max(count, vs.size());
                aggregated.put(k, apply(vs));
            }
            out.set("payload", aggregated);
            ObjectNode info = out.putObject("aggregate");
            info.put("fn", fn);
            info.put("window", window.toString());
            info.put("groupBy", groupBy);
            info.put("count", count);
            ctx.emit("out", new FlowMessage(out, message.triggerMessageId(), key));
        }

        double apply(List<Double> vs) {
            return switch (fn) {
                case "min" -> vs.stream().mapToDouble(Double::doubleValue).min().orElse(Double.NaN);
                case "max" -> vs.stream().mapToDouble(Double::doubleValue).max().orElse(Double.NaN);
                case "sum" -> vs.stream().mapToDouble(Double::doubleValue).sum();
                case "count" -> vs.size();
                default -> vs.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
            };
        }
    }
}
