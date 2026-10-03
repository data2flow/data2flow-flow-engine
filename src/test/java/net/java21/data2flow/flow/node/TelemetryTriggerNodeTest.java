package net.java21.data2flow.flow.node;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import net.java21.data2flow.flow.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static net.java21.data2flow.flow.support.FlowTestHarness.outputs;
import static org.assertj.core.api.Assertions.assertThat;

/** FLW-02 {@code trigger.telemetry}: TC-FLW-033 */
class TelemetryTriggerNodeTest {

    /** 공간 트리(SpaceFixtures.campus 대역): 실습동(10) 아래 실습실(31)·(32). 기기 101·102는 31, 201은 32, 301은 다른 건물 */
    private static final SpaceDirectory CAMPUS = (org, space, descendants) -> {
        if (space == 10 && descendants) {
            return Set.of(101L, 102L, 201L);
        }
        if (space == 31) {
            return Set.of(101L, 102L);
        }
        return Set.of();
    };

    private static String flow(String target, String metrics) {
        return """
                {"schema":"data2flow.flow-definition/v1",
                 "nodes":[{"id":"n-trg00001","type":"trigger.telemetry","config":{"target":%s,"metrics":%s}},
                          {"id":"n-dbg00001","type":"debug.log","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-dbg00001"}]}""".formatted(target, metrics);
    }

    private static int fired(FlowTestHarness h, CanonicalTelemetry t) {
        return h.send(t).size();
    }

    @Test
    @DisplayName("[FLW-05.01] TC-FLW-033 공간(실습동)+measures+하위 포함, temperature: 하위 공간 기기만, temperature만 out 1건")
    void spaceWithChildren() {
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition(flow(
                "{\"spaceId\":\"10\",\"relation\":\"measures\",\"includeChildren\":true}", "[\"temperature\"]")), CAMPUS, 1);

        assertThat(fired(h, FlowFixtures.telemetry(201, 32L, "temperature", 25, MutableClock.T0))).isEqualTo(1);
        assertThat(fired(h, FlowFixtures.telemetry(301, 99L, "temperature", 25, MutableClock.T0))).isZero();
        assertThat(fired(h, FlowFixtures.telemetry(101, 31L, "humidity", 40, MutableClock.T0))).isZero();
    }

    @Test
    @DisplayName("[FLW-05.01] TC-FLW-033 하위 미포함 공간은 메시지의 spaceId 또는 측정 관계 기기로 판정, 모델·기기 목록 대상")
    void otherTargets() {
        FlowTestHarness space = FlowTestHarness.of(FlowFixtures.definition(flow("{\"spaceId\":31}", "[]")), CAMPUS, 1);
        FlowTestHarness model = FlowTestHarness.of(FlowFixtures.definition(flow("{\"modelId\":\"em300-th\"}", "[]")), CAMPUS, 1);
        FlowTestHarness devices = FlowTestHarness.of(FlowFixtures.definition(flow("{\"deviceIds\":[\"7\",8]}", "[]")), CAMPUS, 1);

        assertThat(fired(space, FlowFixtures.telemetry(102, null, "temperature", 25, MutableClock.T0))).isEqualTo(1);
        assertThat(fired(space, FlowFixtures.telemetry(500, 31L, "temperature", 25, MutableClock.T0))).isEqualTo(1);
        assertThat(fired(space, FlowFixtures.telemetry(201, 32L, "temperature", 25, MutableClock.T0))).isZero();
        assertThat(fired(model, FlowFixtures.telemetry(5, 1L, "co2", 800, MutableClock.T0))).isEqualTo(1);
        assertThat(fired(devices, FlowFixtures.telemetry(8, 1L, "co2", 800, MutableClock.T0))).isEqualTo(1);
        assertThat(fired(devices, FlowFixtures.telemetry(9, 1L, "co2", 800, MutableClock.T0))).isZero();
    }

    @Test
    @DisplayName("[FLW-05.01] INACTIVE 기기·다른 조직 메시지는 트리거하지 않고, 메시지는 FLW-api §5.1 모양(payload 측정 키→값, 대상 키 device:id)")
    void messageShape() {
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition(flow("{\"modelId\":\"em300-th\"}", "[]")));
        CanonicalTelemetry inactive = CanonicalTelemetry.builder().organizationId(1).sourceId(3).externalId("x").deviceId(4)
                .deviceStatus(CanonicalTelemetry.DeviceStatus.INACTIVE).modelId("em300-th").measuredAt(MutableClock.T0)
                .receivedAt(MutableClock.T0).metric(CanonicalTelemetry.Metric.of("temperature", 20, "℃")).rawMessageId(1).build();
        CanonicalTelemetry otherOrg = CanonicalTelemetry.builder().organizationId(2).sourceId(3).externalId("x").deviceId(4)
                .modelId("em300-th").measuredAt(MutableClock.T0).receivedAt(MutableClock.T0)
                .metric(CanonicalTelemetry.Metric.of("temperature", 20, "℃")).rawMessageId(1).build();

        assertThat(fired(h, inactive)).isZero();
        assertThat(fired(h, otherOrg)).isZero();
        CanonicalTelemetry t = FlowFixtures.temperature(15, 28.1, MutableClock.T0);
        var out = outputs(h.send(t), "n-trg00001");
        assertThat(out).singleElement().satisfies(o -> {
            assertThat(o.port()).isEqualTo("out");
            assertThat(o.payload().path("payload").path("temperature").asDouble()).isEqualTo(28.1);
            assertThat(o.payload().path("messageId").asString()).isEqualTo(t.messageId().toString());
            assertThat(o.payload().path("deviceId").asLong()).isEqualTo(15);
            assertThat(o.payload().path("metrics").get(0).path("unit").asString()).isEqualTo("℃");
        });
    }
}
