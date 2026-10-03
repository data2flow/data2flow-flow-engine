package net.java21.data2flow.flow.node.service;

import net.java21.data2flow.contracts.capability.ArgViolation;
import net.java21.data2flow.contracts.capability.CapabilityCatalog;
import net.java21.data2flow.contracts.capability.CommandArgsValidator;
import net.java21.data2flow.contracts.capability.CommandValidation;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandPayload;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandTarget;
import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.contracts.message.ActionRequest;
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
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code action.control}(FLW-02 기기 제어, TC-FLW-053, ADR-009). 명령을 바로 보내지 않고 행동 요청 {@link ActionRequest} v1
 * (kind=COMMAND, 라우팅 키 {@code command})을 아웃박스에 쓴다. 릴레이가 {@code data2flow.actions}로 보내고 action의 제어 창구가
 * 권한·검증·인터락·드라이버를 맡는다(BR-FLW-36: 제어는 이 노드만).
 *
 * <ul>
 *   <li>멱등 키 {@code ActionIdempotencyKeys.flow(flowId, nodeId, triggerMessageId)}, 기기 목록 대상이면 기기마다 분할 인덱스를 붙인다.
 *       <b>버전은 넣지 않는다</b>(BR-FLW-13).</li>
 *   <li>출처 {@code CommandSource.flow(flowId, version, nodeId, triggerMessageId)}, 우선순위는 출처가 정하는 AUTO(BR-FLW-37, 다른 값이면 컴파일 오류).</li>
 *   <li>유효 시각 = 처리 시각 + validitySeconds(기본 600초).</li>
 *   <li>표준 기능은 컴파일할 때 기능 스키마로 인자를 검사한다(BR-ACT-01 1단계. 모델 제약·조직 한계는 action이 실행할 때).</li>
 *   <li>결과 포트(ok·failed, EVT-ACT-01 대기)는 M4에서 만든다. M3는 {@code awaitResult=false}로 보내고 포트로 내보내지 않는다.</li>
 * </ul>
 */
public class ControlActionNodeType implements NodeType {

    public static final String TYPE = "action.control";
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);
    private final CapabilityCatalog catalog;
    private final Duration defaultValidity;

    public ControlActionNodeType(CapabilityCatalog catalog, Duration defaultValidity) {
        this.catalog = catalog;
        this.defaultValidity = defaultValidity;
    }

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        String priority = Jsons.text(config, "priority");
        if (priority != null && !"AUTO".equals(priority)) {
            throw new NodeConfigException("config.priority", "제어 노드의 우선순위는 AUTO로 고정입니다(BR-FLW-37)");
        }
        String capability = Jsons.text(config, "capability");
        String command = Jsons.text(config, "command");
        if (capability == null || capability.isBlank()) {
            throw new NodeConfigException("config.capability", "기능(capability)이 필요합니다");
        }
        if (command == null || command.isBlank()) {
            throw new NodeConfigException("config.command", "명령(command)이 필요합니다");
        }
        Map<String, Object> args = new LinkedHashMap<>();
        JsonNode a = config.get("args");
        if (a != null && !a.isNull()) {
            if (!a.isObject()) {
                throw new NodeConfigException("config.args", "인자(args)는 객체여야 합니다");
            }
            args.putAll(Jsons.MAPPER.convertValue(a, Map.class));
        }
        if (catalog.find(capability).isPresent() || !capability.startsWith("custom.")) {
            CommandValidation v = CommandArgsValidator.validate(catalog, capability, command, args, Map.of(), Map.of());
            if (!v.ok()) {
                ArgViolation first = v.violations().getFirst();
                throw new NodeConfigException("config." + first.field(), "INVALID_CONFIG", first.message());
            }
        }
        List<CommandTarget> targets = targets(config.get("target"), capability);
        JsonNode vs = config.get("validitySeconds");
        Duration validity = vs == null || vs.isNull() ? defaultValidity : Duration.ofSeconds(vs.asLong(0));
        if (validity.toSeconds() < 10 || validity.toSeconds() > 86_400) {
            throw new NodeConfigException("config.validitySeconds", "유효 시간은 10~86400초입니다");
        }
        return new Compiled(node.id(), context.flowId().toString(), context.version(), context.organizationId(),
                List.copyOf(targets), capability, command, java.util.Collections.unmodifiableMap(args), validity);
    }

    private static List<CommandTarget> targets(JsonNode target, String capability) {
        if (target == null || !target.isObject()) {
            throw new NodeConfigException("config.target", "대상(target)이 필요합니다");
        }
        List<CommandTarget> out = new ArrayList<>();
        JsonNode devices = target.get("deviceIds");
        Long single = Jsons.id(target, "deviceId", "config.target.deviceId");
        Long space = Jsons.id(target, "spaceId", "config.target.spaceId");
        int kinds = (devices != null && !devices.isNull() ? 1 : 0) + (single == null ? 0 : 1) + (space == null ? 0 : 1);
        if (kinds != 1) {
            throw new NodeConfigException("config.target", "대상은 deviceIds(또는 deviceId)나 spaceId 중 하나만 정합니다");
        }
        if (single != null) {
            out.add(CommandTarget.device(single));
        } else if (space != null) {
            String relation = Jsons.text(target, "relation");
            if (relation != null && !CommandTarget.RELATION_CONTROLS.equalsIgnoreCase(relation)) {
                throw new NodeConfigException("config.target.relation", "제어 대상 관계는 controls입니다: " + relation);
            }
            String cap = Jsons.text(target, "capability");
            out.add(CommandTarget.space(space, CommandTarget.RELATION_CONTROLS, cap == null ? capability : cap,
                    target.path("includeChildren").asBoolean(false)));
        } else {
            if (!devices.isArray() || devices.isEmpty() || devices.size() > 100) {
                throw new NodeConfigException("config.target.deviceIds", "기기 ID 목록은 1~100개입니다");
            }
            for (int i = 0; i < devices.size(); i++) {
                Double d = Jsons.number(devices.get(i));
                if (d == null || d < 1 || d != Math.floor(d)) {
                    throw new NodeConfigException("config.target.deviceIds[" + i + "]", "기기 ID가 올바르지 않습니다");
                }
                out.add(CommandTarget.device(d.longValue()));
            }
        }
        return out;
    }

    record Compiled(String nodeId, String flowId, int version, long organizationId, List<CommandTarget> targets,
                    String capability, String command, Map<String, Object> args, Duration validity) implements CompiledNode {

        @Override
        public List<String> outputs() {
            return List.of("ok", "failed");
        }

        @Override
        public boolean isAction() {
            return true;
        }

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            Instant now = ctx.now();
            Clock fixed = Clock.fixed(now, ZoneOffset.UTC);
            CommandSource source = CommandSource.flow(flowId, ctx.flowVersion(), nodeId, message.triggerMessageId());
            for (int i = 0; i < targets.size(); i++) {
                String key = targets.size() == 1
                        ? ActionIdempotencyKeys.flow(flowId, nodeId, message.triggerMessageId())
                        : ActionIdempotencyKeys.flow(flowId, nodeId, message.triggerMessageId(), i);
                ActionRequest request = ActionRequest.command(organizationId, key, source, now.plus(validity),
                        new CommandPayload(targets.get(i), capability, command, args, false), fixed);
                ctx.action(new ActionDraft("COMMAND", MessagingNames.EXCHANGE_ACTIONS, request.routingKey(), key,
                        Jsons.MAPPER.valueToTree(request), capability + "." + command + args));
            }
        }
    }
}
