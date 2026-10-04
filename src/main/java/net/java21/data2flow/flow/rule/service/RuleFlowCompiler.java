package net.java21.data2flow.flow.rule.service;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.flow.common.FlowEngineErrorCode;
import net.java21.data2flow.flow.plan.domain.Jsons;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 규칙 → 플로우 컴파일(RUL-01.01, BR-RUL-01, ADR-005: 규칙은 따로 엔진을 두지 않고 표준 플로우로 바꿔 같은 엔진에서 실행). 규칙 버전과 플로우
 * 버전은 1:1이고, 노드 ID는 역할로 고정해 규칙을 고쳐도 같은 역할의 노드 상태(지속 시간 타이머·연속 횟수)가 이어진다(FLW-06.03).
 *
 * <pre>
 * [트리거: 범위(DEVICE·SPACE·MODEL·TAG)] → [공간 집계(spaceAvg·Max·Min·any·all)] → [조건] ─(발생)→ [시간 조건] → [알람 RAISE]
 *                                                                                      └(해제)→ [알람 CLEAR](autoClear)
 * </pre>
 *
 * <ul>
 *   <li>threshold: {@code condition.threshold}(for·clear·repeat, BR-RUL-03·04·05). 해제 기준이 없으면 발생 기준(BR-RUL-04). 상태가 바뀔 때만
 *       내보내도록 {@code for}가 없으면 0초로 둔다.</li>
 *   <li>rateOfChange: {@code condition.rateOfChange}(emit change). noData: {@code condition.noData}(timeout → 발생, restored → 해제).</li>
 *   <li>group(AND·OR, 항목 ≤10): {@code condition.group}(emit change). 그룹 안 for·clear·repeat·aggregate와 중첩 그룹은 거부한다.</li>
 *   <li>anomaly(RUL-01.08)는 ANA 실시간 점수(M7)가 필요해 거부한다.</li>
 *   <li>알림은 넣지 않는다: 알람이 열리면 core-api가 알림 정책으로 알린다(EVT-RUL-03).</li>
 * </ul>
 * 알람 키는 {@code rule:{ruleId}:{대상}}(공간 집계면 대상이 공간)이다.
 */
public class RuleFlowCompiler {

    static final String TRIGGER = "n-rtrg";
    static final String AGGREGATE = "n-ragg0001";
    static final String CONDITION = "n-rcnd0001";
    static final String TIME = "n-rtim0001";
    static final String RAISE = "n-rrai0001";
    static final String CLEAR = "n-rclr0001";
    static final int MAX_ITEMS = 10;

    /** 컴파일 결과(정의와 알람 키 대상 종류) */
    public record Result(FlowDefinition definition, String target) {
    }

    public Result compile(long ruleId, JsonNode rule) {
        List<FieldErrorDetail> errors = new ArrayList<>();
        if (rule == null || !rule.isObject()) {
            throw invalid(List.of(new FieldErrorDetail("rule", "REQUIRED", "규칙이 필요합니다")));
        }
        JsonNode condition = rule.path("condition");
        String kind = condition.path("kind").asString("");
        Set<String> metrics = new LinkedHashSet<>();
        collectMetrics(condition, metrics);

        ArrayNode nodes = Jsons.MAPPER.createArrayNode();
        ArrayNode wires = Jsons.MAPPER.createArrayNode();
        List<String> triggers = triggers(rule.path("scope"), metrics, kind.equals("noData"), nodes, errors);

        String entry;
        String raisePort;
        String clearPort;
        String spaceTarget = null;
        String metric = metrics.size() == 1 ? metrics.iterator().next() : null;
        JsonNode thresholdSnapshot = null;
        switch (kind) {
            case "threshold" -> {
                String aggregate = condition.path("aggregate").asString("perDevice");
                ObjectNode config = threshold(condition, "rule.condition", errors);
                thresholdSnapshot = Jsons.object().put("raise", condition.path("value").asDouble())
                        .put("clear", config.path("clear").asDouble(condition.path("value").asDouble()));
                String fn = spaceFunction(aggregate, condition.path("op").asString(""), errors);
                if (fn != null) {
                    ObjectNode agg = node(nodes, AGGREGATE, "transform.aggregate");
                    agg.putObject("config").put("window", condition.path("aggregateWindow").asString("PT5M")).put("fn", fn)
                            .put("groupBy", "space").put("metric", condition.path("metric").asString());
                    entry = AGGREGATE;
                    wire(wires, AGGREGATE, "out", CONDITION);
                    spaceTarget = "SPACE";
                } else {
                    entry = CONDITION;
                }
                node(nodes, CONDITION, "condition.threshold").set("config", config);
                raisePort = "true";
                clearPort = "false";
            }
            case "rateOfChange" -> {
                ObjectNode config = Jsons.object();
                config.put("metric", text(condition, "metric", "rule.condition.metric", errors));
                config.put("window", condition.path("window").asString("PT10M"));
                config.put("delta", condition.path("delta").asDouble(0));
                config.put("direction", condition.path("direction").asString("any").toLowerCase(Locale.ROOT));
                config.put("emit", "change");
                node(nodes, CONDITION, "condition.rateOfChange").set("config", config);
                entry = CONDITION;
                raisePort = "true";
                clearPort = "false";
            }
            case "noData" -> {
                ObjectNode config = Jsons.object().put("window", condition.path("window").asString("PT30M"));
                if (condition.hasNonNull("metric")) {
                    config.put("metric", condition.get("metric").asString());
                }
                node(nodes, CONDITION, "condition.noData").set("config", config);
                entry = CONDITION;
                raisePort = "timeout";
                clearPort = "restored";
            }
            case "group" -> {
                ObjectNode config = Jsons.object();
                config.put("op", condition.path("op").asString("AND").toUpperCase(Locale.ROOT));
                config.put("emit", "change");
                ArrayNode items = config.putArray("items");
                JsonNode list = condition.path("items");
                if (!list.isArray() || list.isEmpty() || list.size() > MAX_ITEMS) {
                    errors.add(new FieldErrorDetail("rule.condition.items", "LIMIT", "그룹 조건은 1~" + MAX_ITEMS + "개입니다(BR-RUL-21)"));
                } else {
                    int i = 0;
                    for (JsonNode item : list.values()) {
                        String path = "rule.condition.items[" + i++ + "]";
                        if (!"threshold".equals(item.path("kind").asString(""))) {
                            errors.add(new FieldErrorDetail(path + ".kind", "UNSUPPORTED",
                                    "그룹 안에는 임계값 조건만 둘 수 있습니다(중첩 그룹·변화율·무수신은 아직 지원하지 않음)"));
                            continue;
                        }
                        for (String unsupported : List.of("for", "clear", "repeat")) {
                            if (item.hasNonNull(unsupported)) {
                                errors.add(new FieldErrorDetail(path + "." + unsupported, "UNSUPPORTED",
                                        "그룹 안 조건의 " + unsupported + "는 아직 지원하지 않습니다"));
                            }
                        }
                        String aggregate = item.path("aggregate").asString("perDevice");
                        if (!aggregate.equals("perDevice")) {
                            errors.add(new FieldErrorDetail(path + ".aggregate", "UNSUPPORTED", "그룹 안 조건은 기기별(perDevice)만 지원합니다"));
                        }
                        ObjectNode it = items.addObject();
                        it.put("metric", text(item, "metric", path + ".metric", errors));
                        it.put("op", item.path("op").asString(">"));
                        if (item.has("range")) {
                            it.set("range", item.get("range"));
                        } else {
                            it.put("value", item.path("value").asDouble());
                        }
                    }
                }
                node(nodes, CONDITION, "condition.group").set("config", config);
                entry = CONDITION;
                raisePort = "true";
                clearPort = "false";
            }
            case "anomaly" -> {
                errors.add(new FieldErrorDetail("rule.condition.kind", "UNSUPPORTED",
                        "이상 탐지 조건은 ANA 실시간 이상 점수가 필요합니다(RUL-01.08, M7)"));
                entry = CONDITION;
                raisePort = "true";
                clearPort = "false";
            }
            default -> {
                errors.add(new FieldErrorDetail("rule.condition.kind", "INVALID",
                        "조건 종류는 threshold, rateOfChange, noData, group 중 하나입니다: " + kind));
                entry = CONDITION;
                raisePort = "true";
                clearPort = "false";
            }
        }
        for (String t : triggers) {
            wire(wires, t, "out", entry);
        }

        // 발생: (시간 조건) → 알람 RAISE
        String raiseFrom = CONDITION;
        String raiseFromPort = raisePort;
        JsonNode time = rule.get("timeCondition");
        if (time != null && time.isObject()) {
            ObjectNode t = node(nodes, TIME, "condition.timeWindow");
            ObjectNode config = t.putObject("config");
            if (time.has("days")) {
                config.set("days", time.get("days"));
            }
            for (String f : List.of("from", "to", "timezone")) {
                if (time.hasNonNull(f)) {
                    config.put(f, time.get(f).asString());
                }
            }
            if (time.hasNonNull("spaceSchedule")) {
                errors.add(new FieldErrorDetail("rule.timeCondition.spaceSchedule", "UNSUPPORTED",
                        "공간 운영 시간표 조건은 아직 지원하지 않습니다(DEV-01.02 내부 API 필요)"));
            }
            if (time.path("invert").asBoolean(false)) {
                config.put("invert", true);
            }
            wire(wires, CONDITION, raisePort, TIME);
            raiseFrom = TIME;
            raiseFromPort = "true";
        }
        String severity = rule.path("severity").asString("");
        if (!List.of("CRITICAL", "MAJOR", "MINOR", "WARNING", "INFO").contains(severity)) {
            errors.add(new FieldErrorDetail("rule.severity", "INVALID", "심각도는 CRITICAL·MAJOR·MINOR·WARNING·INFO입니다: " + severity));
        }
        String title = rule.path("titleTemplate").asString("");
        if (title.isBlank() || title.length() > 200) {
            errors.add(new FieldErrorDetail("rule.titleTemplate", "INVALID", "제목은 1~200자입니다"));
        }
        ObjectNode raise = node(nodes, RAISE, "action.alarm");
        ObjectNode rc = raise.putObject("config").put("mode", "raise").put("severity", severity)
                .put("title", metric == null ? title : title.replace("{{value}}", "{{payload." + metric + "}}"))
                .put("ruleId", Long.toString(ruleId));
        if (metric != null) {
            rc.put("metric", metric);
        }
        if (thresholdSnapshot != null) {
            rc.set("threshold", thresholdSnapshot);
        }
        wire(wires, raiseFrom, raiseFromPort, RAISE);
        if (rule.path("autoClear").asBoolean(true)) {
            ObjectNode clear = node(nodes, CLEAR, "action.alarm");
            ObjectNode cc = clear.putObject("config").put("mode", "clear").put("ruleId", Long.toString(ruleId));
            if (metric != null) {
                cc.put("metric", metric);
            }
            wire(wires, CONDITION, clearPort, CLEAR);
        }
        if (!errors.isEmpty()) {
            throw invalid(errors);
        }
        ObjectNode def = Jsons.object();
        def.put("schema", FlowDefinition.SCHEMA);
        def.putObject("mode").put("concurrency", "queued").put("keyBy", "deviceId").put("max", 10);
        def.set("nodes", nodes);
        def.set("wires", wires);
        return new Result(Jsons.MAPPER.treeToValue(def, FlowDefinition.class), spaceTarget == null ? "DEVICE" : spaceTarget);
    }

    private static ObjectNode threshold(JsonNode c, String path, List<FieldErrorDetail> errors) {
        ObjectNode config = Jsons.object();
        config.put("metric", text(c, "metric", path + ".metric", errors));
        String op = c.path("op").asString(">");
        config.put("op", op);
        if (c.has("range")) {
            config.set("range", c.get("range"));
        } else if (c.hasNonNull("value")) {
            config.put("value", c.get("value").asDouble());
        } else {
            errors.add(new FieldErrorDetail(path + ".value", "REQUIRED", "기준값이 필요합니다"));
        }
        // 상태가 바뀔 때만 내보내도록 지속 시간이 없으면 0초(BR-RUL-03), 해제 기준이 없으면 발생 기준(BR-RUL-04)
        config.put("for", c.path("for").asString("PT0S"));
        if (c.hasNonNull("clear")) {
            config.put("clear", c.get("clear").asDouble());
        } else if (c.hasNonNull("value") && List.of(">", ">=", "<", "<=").contains(op)) {
            config.put("clear", c.get("value").asDouble() + (op.startsWith(">") ? (op.equals(">") ? 0 : -1e-9) : (op.equals("<") ? 0 : 1e-9)));
        }
        if (c.hasNonNull("repeat")) {
            config.put("repeat", c.get("repeat").asInt());
        }
        return config;
    }

    /** 공간 집계 함수. 기기별이면 null */
    private static String spaceFunction(String aggregate, String op, List<FieldErrorDetail> errors) {
        boolean greater = op.startsWith(">");
        boolean less = op.startsWith("<");
        return switch (aggregate) {
            case "perDevice" -> null;
            case "spaceAvg" -> "avg";
            case "spaceMax" -> "max";
            case "spaceMin" -> "min";
            case "any", "all" -> {
                if (!greater && !less) {
                    errors.add(new FieldErrorDetail("rule.condition.aggregate", "UNSUPPORTED",
                            "any·all은 >, >=, <, <= 비교에서만 씁니다"));
                    yield null;
                }
                // 하나라도 넘음 = 최댓값이 넘음, 모두 넘음 = 최솟값이 넘음(작다 비교는 반대)
                yield aggregate.equals("any") == greater ? "max" : "min";
            }
            default -> {
                errors.add(new FieldErrorDetail("rule.condition.aggregate", "INVALID", "집계는 perDevice, spaceAvg, spaceMax, "
                        + "spaceMin, any, all 중 하나입니다: " + aggregate));
                yield null;
            }
        };
    }

    private static List<String> triggers(JsonNode scope, Set<String> metrics, boolean anyMessage, ArrayNode nodes,
                                         List<FieldErrorDetail> errors) {
        String type = scope.path("type").asString("");
        JsonNode ids = scope.path("ids");
        List<String> out = new ArrayList<>();
        if (!ids.isArray() || ids.isEmpty()) {
            errors.add(new FieldErrorDetail("rule.scope.ids", "REQUIRED", "적용 범위가 필요합니다"));
            return out;
        }
        List<ObjectNode> targets = new ArrayList<>();
        switch (type) {
            case "DEVICE" -> {
                ObjectNode t = Jsons.object();
                ArrayNode d = t.putArray("deviceIds");
                ids.values().forEach(v -> d.add(v.asString()));
                targets.add(t);
            }
            case "SPACE" -> ids.values().forEach(v -> targets.add(Jsons.object().put("spaceId", v.asString()).put("relation", "measures")
                    .put("includeChildren", scope.path("includeChildren").asBoolean(true))));
            case "MODEL" -> ids.values().forEach(v -> targets.add(Jsons.object().put("modelId", v.asString())));
            case "TAG" -> {
                ObjectNode t = Jsons.object();
                ArrayNode tags = t.putArray("tags");
                ids.values().forEach(v -> tags.add(v.asString()));
                targets.add(t);
            }
            default -> errors.add(new FieldErrorDetail("rule.scope.type", "INVALID", "범위는 DEVICE, SPACE, MODEL, TAG 중 하나입니다: " + type));
        }
        int i = 1;
        for (ObjectNode target : targets) {
            String id = String.format("%s%04d", TRIGGER, i++);
            ObjectNode trigger = node(nodes, id, "trigger.telemetry");
            ObjectNode config = trigger.putObject("config");
            config.set("target", target);
            if (!anyMessage && !metrics.isEmpty()) {
                ArrayNode m = config.putArray("metrics");
                metrics.forEach(m::add);
            }
            out.add(id);
        }
        return out;
    }

    private static void collectMetrics(JsonNode condition, Set<String> metrics) {
        if (condition.hasNonNull("metric")) {
            metrics.add(condition.get("metric").asString());
        }
        condition.path("items").values().forEach(i -> collectMetrics(i, metrics));
    }

    private static String text(JsonNode node, String field, String path, List<FieldErrorDetail> errors) {
        String v = node.path(field).asString("");
        if (v.isBlank()) {
            errors.add(new FieldErrorDetail(path, "REQUIRED", field + "이(가) 필요합니다"));
        }
        return v;
    }

    private static ObjectNode node(ArrayNode nodes, String id, String type) {
        ObjectNode n = nodes.addObject();
        n.put("id", id);
        n.put("type", type);
        n.put("typeVersion", 1);
        return n;
    }

    private static void wire(ArrayNode wires, String from, String port, String to) {
        wires.addObject().put("from", from).put("port", port).put("to", to);
    }

    private static BusinessException invalid(List<FieldErrorDetail> errors) {
        return new BusinessException(FlowEngineErrorCode.RULE_CONDITION_INVALID, errors);
    }
}
