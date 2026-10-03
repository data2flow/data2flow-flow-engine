package net.java21.data2flow.flow.node.service;

import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/** 노드들이 함께 쓰는 메시지 읽기 */
final class Messages {

    private Messages() {
    }

    /** 측정 시각(이벤트 시각). 없거나 형식이 틀리면 처리 시각 */
    static Instant measuredAt(FlowMessage message, Instant fallback) {
        String at = Jsons.text(message.body(), "measuredAt");
        if (at == null) {
            return fallback;
        }
        try {
            return Instant.parse(at);
        } catch (DateTimeParseException e) {
            return fallback;
        }
    }

    /**
     * 값 읽기: {@code metric}이 있으면 {@code payload.<metric>}(없으면 {@code metrics[key=metric].value}), 없으면 payload 자체.
     * 메시지에 그 값이 없으면 MissingNode.
     */
    static JsonNode value(FlowMessage message, String metric) {
        JsonNode payload = message.body().path("payload");
        if (metric == null) {
            return payload;
        }
        JsonNode v = payload.isObject() ? payload.get(metric) : null;
        if (v != null && !v.isNull()) {
            return v;
        }
        JsonNode metrics = message.body().get("metrics");
        if (metrics != null && metrics.isArray()) {
            for (JsonNode m : metrics.values()) {
                if (metric.equals(Jsons.text(m, "key"))) {
                    return m.path("value");
                }
            }
        }
        return tools.jackson.databind.node.MissingNode.getInstance();
    }
}
