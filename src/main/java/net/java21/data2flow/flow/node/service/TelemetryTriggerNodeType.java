package net.java21.data2flow.flow.node.service;

import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.plan.domain.CompileContext;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.NodeConfigException;
import net.java21.data2flow.flow.plan.domain.NodeType;
import net.java21.data2flow.flow.plan.domain.TriggerNode;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * {@code trigger.telemetry}(FLW-02 트리거, FLW-05.01, TC-FLW-033). 대상은 기기 목록, 공간(+관계 measures, 하위 공간 포함), 모델 중 하나이고
 * {@code metrics}가 있으면 그 항목 중 하나라도 들어 있는 메시지만 받는다. INACTIVE 기기의 메시지는 받지 않는다(CanonicalTelemetry 계약).
 *
 * <p>공간 대상은 {@link SpaceDirectory}(core API-DEV-128 캐시)가 준 기기 목록 또는 메시지의 {@code spaceId}로 판정한다. 태그 대상
 * ({@code target.tags}, M4)은 기기 태그(core API-DEV-122 {@code tags[]}, 캐시: 처음 본 기기는 읽어 오는 동안 메시지 태그로만 판정) 또는
 * 메시지 {@code meta.tags}의 값·{@code 키:값}이 하나라도 겹치면 받는다.
 *
 * <p>내보내는 메시지: {@code {messageId, topic:"telemetry", organizationId, deviceId, spaceId, modelId, measuredAt, receivedAt, virtual,
 * payload:{키: 값}, metrics:[원본 측정값]}}, 대상 키 {@code device:{deviceId}}.
 */
public class TelemetryTriggerNodeType implements NodeType {

    public static final String TYPE = "trigger.telemetry";
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);
    private final SpaceDirectory spaces;

    public TelemetryTriggerNodeType(SpaceDirectory spaces) {
        this.spaces = spaces;
    }

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public TriggerNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config();
        JsonNode target = config == null ? null : config.get("target");
        if (target == null || !target.isObject()) {
            throw new NodeConfigException("config.target", "대상(target)이 필요합니다");
        }
        Set<Long> deviceIds = new HashSet<>();
        JsonNode devices = target.get("deviceIds");
        if (devices != null && !devices.isNull()) {
            if (!devices.isArray() || devices.isEmpty()) {
                throw new NodeConfigException("config.target.deviceIds", "기기 ID 목록은 비어 있지 않은 배열이어야 합니다");
            }
            for (int i = 0; i < devices.size(); i++) {
                Double d = Jsons.number(devices.get(i));
                if (d == null || d < 1 || d != Math.floor(d)) {
                    throw new NodeConfigException("config.target.deviceIds[" + i + "]", "기기 ID가 올바르지 않습니다: " + devices.get(i));
                }
                deviceIds.add(d.longValue());
            }
        }
        Long spaceId = Jsons.id(target, "spaceId", "config.target.spaceId");
        String modelId = Jsons.text(target, "modelId");
        Set<String> tags = new LinkedHashSet<>();
        JsonNode t = target.get("tags");
        if (t != null && !t.isNull()) {
            if (!t.isArray() || t.isEmpty()) {
                throw new NodeConfigException("config.target.tags", "태그 목록은 비어 있지 않은 배열이어야 합니다");
            }
            t.values().forEach(v -> {
                if (!v.asString("").isBlank()) {
                    tags.add(v.asString().trim());
                }
            });
        }
        String relation = Jsons.text(target, "relation");
        if (relation != null && !"measures".equalsIgnoreCase(relation)) {
            throw new NodeConfigException("config.target.relation", "트리거 공간 관계는 measures만 씁니다: " + relation);
        }
        int kinds = (deviceIds.isEmpty() ? 0 : 1) + (spaceId == null ? 0 : 1) + (modelId == null ? 0 : 1) + (tags.isEmpty() ? 0 : 1);
        if (kinds != 1) {
            throw new NodeConfigException("config.target", "대상은 deviceIds, spaceId, modelId, tags 중 하나만 정합니다");
        }
        boolean includeChildren = target.path("includeChildren").asBoolean(false);
        Set<String> metrics = new LinkedHashSet<>();
        JsonNode m = config.get("metrics");
        if (m != null && m.isArray()) {
            m.values().forEach(v -> {
                if (v.isString() && !v.stringValue().isBlank()) {
                    metrics.add(v.stringValue());
                }
            });
        }
        boolean includeVirtual = config.path("includeVirtual").asBoolean(true);
        if (spaceId != null) {
            try {
                spaces.warm(context.organizationId(), spaceId, includeChildren);
            } catch (RuntimeException e) {
                // core가 잠시 응답하지 않아도 컴파일은 계속한다(메시지의 spaceId로 판정, 캐시는 나중에 채움)
            }
        }
        return new Compiled(context.organizationId(), Set.copyOf(deviceIds), spaceId, modelId, includeChildren,
                Set.copyOf(metrics), includeVirtual, spaces, Set.copyOf(tags));
    }

    record Compiled(long organizationId, Set<Long> deviceIds, Long spaceId, String modelId, boolean includeChildren,
                    Set<String> metrics, boolean includeVirtual, SpaceDirectory spaces, Set<String> tags) implements TriggerNode {

        /** 태그 대상: 기기 태그(core, 캐시) 또는 메시지 {@code meta.tags}의 값·{@code 키:값}이 하나라도 겹치면 */
        boolean tagged(CanonicalTelemetry t) {
            for (String tag : spaces.deviceTags(organizationId, t.deviceId())) {
                if (tags.contains(tag)) {
                    return true;
                }
            }
            if (t.meta() != null && t.meta().tags() != null) {
                for (var e : t.meta().tags().entrySet()) {
                    if (tags.contains(e.getValue()) || tags.contains(e.getKey() + ":" + e.getValue())) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public List<String> outputs() {
            return List.of("out");
        }

        @Override
        public Optional<FlowMessage> match(CanonicalTelemetry t) {
            if (t.organizationId() != organizationId || t.deviceStatus() == CanonicalTelemetry.DeviceStatus.INACTIVE
                    || (!includeVirtual && t.virtual())) {
                return Optional.empty();
            }
            if (!metrics.isEmpty() && t.metrics().stream().noneMatch(x -> metrics.contains(x.key()))) {
                return Optional.empty();
            }
            boolean inTarget;
            if (!deviceIds.isEmpty()) {
                inTarget = deviceIds.contains(t.deviceId());
            } else if (spaceId != null) {
                inTarget = (!includeChildren && spaceId.equals(t.spaceId()))
                        || spaces.measuringDevices(organizationId, spaceId, includeChildren).contains(t.deviceId());
            } else if (modelId != null) {
                inTarget = modelId.equals(t.modelId());
            } else {
                inTarget = tagged(t);
            }
            return inTarget ? Optional.of(toMessage(t)) : Optional.empty();
        }
    }

    /** 텔레메트리 → 플로우 메시지(FLW-api §5.1) */
    public static FlowMessage toMessage(CanonicalTelemetry t) {
        ObjectNode body = Jsons.object();
        body.put("messageId", t.messageId().toString());
        body.put("topic", "telemetry");
        body.put("organizationId", t.organizationId());
        body.put("deviceId", t.deviceId());
        if (t.spaceId() != null) {
            body.put("spaceId", t.spaceId());
        }
        if (t.modelId() != null) {
            body.put("modelId", t.modelId());
        }
        body.put("measuredAt", t.measuredAt().toString());
        body.put("receivedAt", t.receivedAt().toString());
        body.put("virtual", t.virtual());
        ObjectNode payload = body.putObject("payload");
        ArrayNode metrics = body.putArray("metrics");
        for (CanonicalTelemetry.Metric metric : t.metrics()) {
            payload.put(metric.key(), metric.value());
            ObjectNode item = metrics.addObject();
            item.put("key", metric.key());
            item.put("value", metric.value());
            if (metric.unit() != null) {
                item.put("unit", metric.unit());
            }
            item.put("quality", metric.quality());
        }
        return new FlowMessage(body, t.messageId().toString(), "device:" + t.deviceId());
    }
}
