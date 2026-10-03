package net.java21.data2flow.flow.support;

import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.plan.domain.Jsons;

import java.time.Instant;
import java.util.UUID;

/**
 * 시험용 플로우 정의·텔레메트리(FLW test-plan "FlowFixtures"). 정의는 편집기(data2flow-web)가 만드는 모양과 같다(노드 ID {@code n-} + 8자,
 * 기간 ISO-8601).
 */
public final class FlowFixtures {

    public static final long ORG = 1;
    public static final long SPACE = 31;
    public static final UUID FLOW = UUID.fromString("7f3a0000-0000-4000-8000-000000000001");

    private FlowFixtures() {
    }

    public static FlowDefinition definition(String json) {
        return Jsons.MAPPER.readValue(json, FlowDefinition.class);
    }

    /**
     * 템플릿 "고온이면 냉방"(FLW-01.05, M3 시연): 공간 측정 트리거 → 공간 평균(5분) → 임계값(> value, for, clear) → 에어컨 냉방.
     */
    public static FlowDefinition cooling(double value, String forDuration, double target) {
        return definition("""
                {"schema":"data2flow.flow-definition/v1","mode":{"concurrency":"queued","keyBy":"deviceId","max":10},
                 "nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","typeVersion":1,"name":"실습실 온도","config":{"target":{"spaceId":"%d","relation":"measures","includeChildren":false},"metrics":["temperature"]},"position":{"x":64,"y":96}},
                  {"id":"n-agg00001","type":"transform.aggregate","typeVersion":1,"name":"평균","config":{"window":"PT5M","fn":"avg","groupBy":"space"},"position":{"x":288,"y":96}},
                  {"id":"n-thr00001","type":"condition.threshold","typeVersion":1,"name":"온도>%s","config":{"metric":"temperature","op":">","value":%s,"for":"%s","clear":%s},"position":{"x":512,"y":96}},
                  {"id":"n-act00001","type":"action.control","typeVersion":1,"name":"에어컨 냉방","config":{"target":{"spaceId":"%d","relation":"controls","capability":"Thermostat"},"capability":"Thermostat","command":"set","args":{"mode":"cool","targetTemperature":%s},"validitySeconds":600},"position":{"x":736,"y":96}}],
                 "wires":[{"from":"n-trg00001","port":"out","to":"n-agg00001"},{"from":"n-agg00001","port":"out","to":"n-thr00001"},
                          {"from":"n-thr00001","port":"true","to":"n-act00001"}]}
                """.formatted(SPACE, value, value, forDuration, value - 1, SPACE, target));
    }

    public static CanonicalTelemetry temperature(long deviceId, double value, Instant measuredAt) {
        return telemetry(deviceId, SPACE, "temperature", value, measuredAt);
    }

    public static CanonicalTelemetry telemetry(long deviceId, Long spaceId, String metric, double value, Instant measuredAt) {
        return CanonicalTelemetry.builder()
                .organizationId(ORG).sourceId(3).externalId("dev-" + deviceId).deviceId(deviceId)
                .modelId("em300-th").spaceId(spaceId).measuredAt(measuredAt).receivedAt(measuredAt)
                .metric(CanonicalTelemetry.Metric.of(metric, value, "temperature".equals(metric) ? "℃" : null))
                .rawMessageId(1000 + deviceId).build();
    }
}
