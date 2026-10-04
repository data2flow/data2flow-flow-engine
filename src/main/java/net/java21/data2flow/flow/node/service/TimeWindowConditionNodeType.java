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

import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * {@code condition.timeWindow}(시간 조건, RUL-01.07·TC-RUL-020): 메시지의 측정 시각을 조직 시간대(기본 Asia/Seoul)로 바꿔 요일·시각 범위
 * 안이면 {@code true}, 밖이면 {@code false}(시작 포함·끝 배타). {@code from ≥ to}면 자정을 넘는 범위(22:00~06:00). {@code invert}면 반대(운영 시간 외).
 * 공간 운영 시간표({@code spaceSchedule}, DEV-01.02)는 아직 쓰지 않는다(core 내부 API 없음).
 */
public class TimeWindowConditionNodeType implements NodeType {

    public static final String TYPE = "condition.timeWindow";
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        if (config.hasNonNull("spaceSchedule")) {
            throw new NodeConfigException("config.spaceSchedule", "공간 운영 시간표 조건은 아직 지원하지 않습니다(DEV-01.02 내부 API 필요)");
        }
        Set<DayOfWeek> days = EnumSet.noneOf(DayOfWeek.class);
        JsonNode d = config.get("days");
        if (d != null && d.isArray()) {
            for (JsonNode day : d.values()) {
                days.add(day(day.asString("")));
            }
        }
        if (days.isEmpty()) {
            days = EnumSet.allOf(DayOfWeek.class);
        }
        LocalTime from = time(Jsons.text(config, "from"), "config.from", LocalTime.MIN);
        LocalTime to = time(Jsons.text(config, "to"), "config.to", LocalTime.MAX);
        ZoneId zone;
        try {
            zone = ZoneId.of(Jsons.text(config, "timezone") == null ? "Asia/Seoul" : Jsons.text(config, "timezone"));
        } catch (DateTimeException e) {
            throw new NodeConfigException("config.timezone", "시간대가 올바르지 않습니다: " + Jsons.text(config, "timezone"));
        }
        return new Compiled(Set.copyOf(days), from, to, zone, config.path("invert").asBoolean(false));
    }

    static DayOfWeek day(String text) {
        String t = text.trim().toUpperCase(Locale.ROOT);
        for (DayOfWeek d : DayOfWeek.values()) {
            if (d.name().startsWith(t) && t.length() >= 3) {
                return d;
            }
        }
        throw new NodeConfigException("config.days", "요일은 MON~SUN입니다: " + text);
    }

    private static LocalTime time(String text, String path, LocalTime fallback) {
        if (text == null || text.isBlank()) {
            return fallback;
        }
        try {
            return LocalTime.parse(text.length() == 5 ? text + ":00" : text);
        } catch (DateTimeParseException e) {
            throw new NodeConfigException(path, "시각은 HH:mm 형식입니다: " + text);
        }
    }

    record Compiled(Set<DayOfWeek> days, LocalTime from, LocalTime to, ZoneId zone, boolean invert) implements CompiledNode {

        @Override
        public List<String> outputs() {
            return List.of("true", "false");
        }

        /** 시작 포함·끝 배타(09:00~18:00이면 18:00은 밖). 자정을 넘는 범위의 이른 부분은 전날 요일로 본다 */
        boolean inside(ZonedDateTime t) {
            LocalTime time = t.toLocalTime();
            if (!from.isBefore(to)) {
                if (!time.isBefore(from)) {
                    return days.contains(t.getDayOfWeek());
                }
                return time.isBefore(to) && days.contains(t.getDayOfWeek().minus(1));
            }
            return !time.isBefore(from) && time.isBefore(to) && days.contains(t.getDayOfWeek());
        }

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            ZonedDateTime t = Messages.measuredAt(message, ctx.now()).atZone(zone);
            boolean in = inside(t) != invert;
            ctx.emit(in ? "true" : "false", message);
        }
    }
}
