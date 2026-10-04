package net.java21.data2flow.flow.node.service;

import net.java21.data2flow.contracts.alarm.AlarmKeys;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.alarm.AlarmSourceType;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.AlarmSignal;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.flow.plan.domain.ActionDraft;
import net.java21.data2flow.flow.plan.domain.CompileContext;
import net.java21.data2flow.flow.plan.domain.CompiledNode;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.NodeConfigException;
import net.java21.data2flow.flow.plan.domain.NodeContext;
import net.java21.data2flow.flow.plan.domain.NodeType;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@code action.alarm}(FLW-02 알람, TC-FLW-054, RUL-01.01 규칙 컴파일의 알람 단계): 알람 발생·해제 요청 EVT-RUL-01 {@code alarm.signal}
 * ({@link AlarmSignal})을 아웃박스로 낸다(topic {@code data2flow.events}). core-api 알람 서비스가 알람 키로 열린 알람을 만들거나 갱신·해제한다
 * (BR-RUL-02: 같은 키에 열린 알람은 하나). 앞 조건 노드가 상태가 바뀔 때만 내보내므로(임계값 for·clear, 무수신, edge) 이 노드는 상태가 없다.
 *
 * <ul>
 *   <li>알람 키: 규칙이 컴파일된 플로우면({@code ruleId}) {@code rule:{ruleId}:{대상}}, 아니면 {@code flow:{flowId}:{nodeId}:{대상}}.
 *       대상은 기기 ID, 기기가 없으면 {@code space-{spaceId}}({@link AlarmKeys#target}).</li>
 *   <li>{@code mode}: raise(기본) 또는 clear. raise에는 {@code severity}·{@code title}(메시지 경로 자리표시자 {@code {{payload.co2}}})이 필요하다.</li>
 *   <li>값: {@code metric}이 있으면 그 측정값, 기준 스냅숏 {@code threshold{raise, clear}}(BR-RUL-04).</li>
 * </ul>
 */
public class AlarmActionNodeType implements NodeType {

    public static final String TYPE = "action.alarm";
    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{\\s*([^}]+?)\\s*}}");
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        String mode = Jsons.text(config, "mode") == null ? "raise" : Jsons.text(config, "mode").toLowerCase(Locale.ROOT);
        if (!mode.equals("raise") && !mode.equals("clear")) {
            throw new NodeConfigException("config.mode", "알람 동작은 raise 또는 clear입니다: " + mode);
        }
        AlarmSeverity severity = null;
        String title = Jsons.text(config, "title");
        if (mode.equals("raise")) {
            String sev = Jsons.text(config, "severity");
            try {
                severity = sev == null ? null : AlarmSeverity.valueOf(sev.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                severity = null;
            }
            if (severity == null || severity == AlarmSeverity.UNKNOWN) {
                throw new NodeConfigException("config.severity", "심각도(CRITICAL·MAJOR·MINOR·WARNING·INFO)가 필요합니다: " + sev);
            }
            if (title == null || title.isBlank() || title.length() > 200) {
                throw new NodeConfigException("config.title", "제목(title)은 1~200자입니다");
            }
        }
        Long ruleId = Jsons.id(config, "ruleId", "config.ruleId");
        JsonNode t = config.get("threshold");
        AlarmSignal.Threshold threshold = t == null || !t.isObject() ? null
                : new AlarmSignal.Threshold(Jsons.number(t, "raise"), Jsons.number(t, "clear"));
        return new Compiled(node.id(), context.flowId().toString(), context.organizationId(), mode.equals("raise"), severity,
                title, ruleId, Jsons.text(config, "metric"), threshold);
    }

    record Compiled(String nodeId, String flowId, long organizationId, boolean raise, AlarmSeverity severity, String title,
                    Long ruleId, String metric, AlarmSignal.Threshold threshold) implements CompiledNode {

        private static final MessageCodec CODEC = MessageCodec.create();

        @Override
        public List<String> outputs() {
            return List.of("out");
        }

        @Override
        public boolean isAction() {
            return true;
        }

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            Long deviceId = id(message.body().get("deviceId"));
            Long spaceId = id(message.body().get("spaceId"));
            if (deviceId == null && spaceId == null) {
                String key = message.targetKey();
                if (key.startsWith("space:")) {
                    spaceId = id(Jsons.NODES.stringNode(key.substring("space:".length())));
                } else if (key.startsWith("device:")) {
                    deviceId = id(Jsons.NODES.stringNode(key.substring("device:".length())));
                }
            }
            if (deviceId == null && spaceId == null) {
                ctx.fail(message, "ALARM_TARGET_MISSING", "알람 대상(기기·공간)을 메시지에서 찾지 못했습니다");
                return;
            }
            String target = AlarmKeys.target(deviceId, spaceId);
            String alarmKey = ruleId != null ? AlarmKeys.rule(ruleId, target) : AlarmKeys.flow(flowId, nodeId, target);
            AlarmSourceType source = ruleId != null ? AlarmSourceType.RULE : AlarmSourceType.FLOW;
            Double value = null;
            if (metric != null) {
                JsonNode v = Messages.value(message, metric);
                value = v.isNumber() ? v.doubleValue() : null;
            }
            Instant measuredAt = Messages.measuredAt(message, ctx.now());
            AlarmSignal signal = raise
                    ? AlarmSignal.raise(alarmKey, source, ruleId, flowId, ctx.flowVersion(), nodeId, severity, render(title, message),
                    deviceId, spaceId, metric, value, threshold, measuredAt, message.triggerMessageId())
                    : AlarmSignal.clear(alarmKey, source, ruleId, flowId, ctx.flowVersion(), nodeId, deviceId, spaceId, metric, value,
                    measuredAt, message.triggerMessageId());
            DomainEvent<AlarmSignal> event = DomainEvent.of(EventType.ALARM_SIGNAL, organizationId, signal, null,
                    Clock.fixed(ctx.now(), ZoneOffset.UTC));
            String key = ActionIdempotencyKeys.flow(flowId, nodeId, message.triggerMessageId());
            ctx.action(new ActionDraft("EVENT", MessagingNames.EXCHANGE_EVENTS, EventType.ALARM_SIGNAL.routingKey(), key,
                    Jsons.MAPPER.readTree(CODEC.write(event)), (raise ? "alarm.raise " : "alarm.clear ") + alarmKey));
            ctx.emit("out", message);
        }

        private static Long id(JsonNode v) {
            Double d = Jsons.number(v);
            return d == null || d < 1 || d != Math.floor(d) ? null : d.longValue();
        }

        /** {@code {{경로}}}를 메시지 값으로 바꾼다(없으면 빈 문자열). 200자로 자른다 */
        static String render(String template, FlowMessage message) {
            Matcher m = PLACEHOLDER.matcher(template);
            StringBuilder out = new StringBuilder();
            while (m.find()) {
                JsonNode v = Jsons.at(message.body(), Jsons.normalizePath(m.group(1)));
                String text = v.isMissingNode() || v.isNull() ? "" : v.isString() ? v.stringValue() : v.toString();
                m.appendReplacement(out, Matcher.quoteReplacement(text));
            }
            m.appendTail(out);
            return out.length() > 200 ? out.substring(0, 200) : out.toString();
        }
    }
}
