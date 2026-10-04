package net.java21.data2flow.flow.node.service;

import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.notification.NotificationEvents;
import net.java21.data2flow.contracts.notification.NotificationRecipient;
import net.java21.data2flow.contracts.notification.NotificationRequest;
import net.java21.data2flow.flow.plan.domain.ActionDraft;
import net.java21.data2flow.flow.plan.domain.CompileContext;
import net.java21.data2flow.flow.plan.domain.CompiledNode;
import net.java21.data2flow.flow.plan.domain.Durations;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.NodeConfigException;
import net.java21.data2flow.flow.plan.domain.NodeContext;
import net.java21.data2flow.flow.plan.domain.NodeType;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * {@code action.notify}(FLW-02 알림, TC-FLW-055, RUL-03 연동): 알림 요청 {@link NotificationRequest}(kind=NOTIFY, 라우팅 키 {@code notify})를
 * 아웃박스에 쓴다. 채널·정책·묶기·재시도·에스컬레이션은 action의 알림 공통 계층이 한다(ADR-033). 플로우 알림은 알람과 무관하므로
 * {@code alarmId}가 없고 사건은 {@code flow.notify}다.
 *
 * <ul>
 *   <li>수신자: {@code policyId}(정책으로 action이 계산) 또는 {@code channels[]}(채널 기본 대화방) 또는 {@code recipients[{type, id, channel}]}.</li>
 *   <li>템플릿 {@code templateKey}(기본 {@code flow.notify.default}), 변수: 메시지의 {@code deviceId·spaceId·measuredAt·payload}와
 *       {@code variables{이름: 메시지 경로}}.</li>
 *   <li>묶기 {@code aggregateWindow}(0 또는 1~10분) — 같은 플로우·노드·대상 키의 알림을 묶는다.</li>
 *   <li>멱등 키 {@code sha256(flowId, nodeId, 원인 메시지)}(BR-FLW-13). 메시지는 그대로 {@code out}으로 넘긴다.</li>
 * </ul>
 */
public class NotifyActionNodeType implements NodeType {

    public static final String TYPE = "action.notify";
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        Long policyId = Jsons.id(config, "policyId", "config.policyId");
        List<NotificationRecipient> recipients = new ArrayList<>();
        JsonNode channels = config.get("channels");
        if (channels != null && !channels.isNull()) {
            if (!channels.isArray()) {
                throw new NodeConfigException("config.channels", "채널은 문자열 배열입니다");
            }
            for (JsonNode c : channels.values()) {
                String channel = c.asString("").trim().toUpperCase(Locale.ROOT);
                if (channel.isEmpty()) {
                    throw new NodeConfigException("config.channels", "빈 채널 이름이 있습니다");
                }
                recipients.add(NotificationRecipient.channelDefault(channel));
            }
        }
        JsonNode list = config.get("recipients");
        if (list != null && list.isArray()) {
            int i = 0;
            for (JsonNode r : list.values()) {
                try {
                    recipients.add(new NotificationRecipient(NotificationRecipient.Type.valueOf(r.path("type").asString("")),
                            Jsons.text(r, "id"), Jsons.text(r, "channel"), Jsons.text(r, "address")));
                } catch (IllegalArgumentException e) {
                    throw new NodeConfigException("config.recipients[" + i + "]", "수신자가 올바르지 않습니다: " + e.getMessage());
                }
                i++;
            }
        }
        if (policyId == null && recipients.isEmpty()) {
            throw new NodeConfigException("config.policyId", "알림 정책(policyId) 또는 채널(channels)이 필요합니다");
        }
        String template = Jsons.text(config, "templateKey");
        if (template == null || template.isBlank()) {
            template = NotificationEvents.defaultTemplateKey(NotificationEvents.FLOW_NOTIFY);
        }
        Integer window = null;
        String w = Jsons.text(config, "aggregateWindow");
        if (w != null && !w.isBlank()) {
            Duration d = Durations.parse(w, "config.aggregateWindow");
            long seconds = d.toSeconds();
            if (seconds != 0 && (seconds < NotificationRequest.MIN_AGGREGATE_WINDOW_SEC
                    || seconds > NotificationRequest.MAX_AGGREGATE_WINDOW_SEC)) {
                throw new NodeConfigException("config.aggregateWindow", "묶기 창은 0 또는 1~10분입니다: " + w);
            }
            window = (int) seconds;
        }
        AlarmSeverity severity = null;
        String sev = Jsons.text(config, "severity");
        if (sev != null) {
            try {
                severity = AlarmSeverity.valueOf(sev.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new NodeConfigException("config.severity", "심각도가 올바르지 않습니다: " + sev);
            }
        }
        Map<String, String> variables = new LinkedHashMap<>();
        JsonNode vars = config.get("variables");
        if (vars != null && vars.isObject()) {
            vars.properties().forEach(e -> variables.put(e.getKey(), e.getValue().asString("")));
        }
        return new Compiled(node.id(), context.flowId().toString(), context.organizationId(), policyId, List.copyOf(recipients),
                template, window, severity, java.util.Collections.unmodifiableMap(variables), Jsons.text(config, "link"));
    }

    record Compiled(String nodeId, String flowId, long organizationId, Long policyId, List<NotificationRecipient> recipients,
                    String templateKey, Integer aggregateWindowSec, AlarmSeverity severity, Map<String, String> variables,
                    String link) implements CompiledNode {

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
            Map<String, Object> vars = new LinkedHashMap<>();
            vars.put("flowId", flowId);
            vars.put("nodeId", nodeId);
            for (String field : List.of("deviceId", "spaceId", "measuredAt")) {
                String v = Jsons.text(message.body(), field);
                if (v != null) {
                    vars.put(field, v);
                }
            }
            JsonNode payload = message.body().get("payload");
            if (payload != null && !payload.isNull()) {
                vars.put("payload", Jsons.MAPPER.convertValue(payload, Object.class));
            }
            variables.forEach((name, path) -> {
                JsonNode v = Jsons.at(message.body(), Jsons.normalizePath(path));
                if (!v.isMissingNode() && !v.isNull()) {
                    vars.put(name, Jsons.MAPPER.convertValue(v, Object.class));
                }
            });
            NotificationRequest request = new NotificationRequest(null, NotificationEvents.FLOW_NOTIFY, null, severity, policyId,
                    recipients, Map.of(NotificationRequest.DEFAULT_TEMPLATE, templateKey), vars, aggregateWindowSec,
                    "flow:" + flowId + ":" + nodeId + ":" + message.targetKey(), null, link,
                    message.body().path("virtual").asBoolean(false) ? Boolean.TRUE : null);
            String key = ActionIdempotencyKeys.flow(flowId, nodeId, message.triggerMessageId());
            ActionRequest action = ActionRequest.notify(organizationId, key,
                    CommandSource.flow(flowId, ctx.flowVersion(), nodeId, message.triggerMessageId()), null, request,
                    Clock.fixed(ctx.now(), ZoneOffset.UTC));
            ctx.action(new ActionDraft("NOTIFY", MessagingNames.EXCHANGE_ACTIONS, action.routingKey(), key,
                    Jsons.MAPPER.valueToTree(action), "notify " + (policyId != null ? "policy:" + policyId : recipients)));
            ctx.emit("out", message);
        }
    }
}
