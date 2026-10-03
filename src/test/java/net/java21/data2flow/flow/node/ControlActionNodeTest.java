package net.java21.data2flow.flow.node;

import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageSchemas;
import net.java21.data2flow.contracts.test.message.MessageFixtures;
import net.java21.data2flow.flow.runtime.domain.InMemoryExecutionStore;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** FLW-02 {@code action.control}: TC-FLW-053(아웃박스 ActionRequest), BR-FLW-13·37, 계약(TC-ACT-027 공유 픽스처) */
class ControlActionNodeTest {

    private static final String NODE = "n-test0001";
    private static final MessageCodec CODEC = MessageCodec.create();

    @Test
    @DisplayName("[FLW-05.01] TC-FLW-053 공간+controls+Thermostat set(cool,24) → 아웃박스 ActionRequest 1건, AUTO, 유효 600초, 버전 없는 멱등 키")
    void spaceTarget() {
        FlowTestHarness h = FlowTestHarness.node("action.control", """
                {"target":{"spaceId":"31","relation":"controls","capability":"Thermostat"},"capability":"Thermostat","command":"set",
                 "args":{"mode":"cool","targetTemperature":24},"validitySeconds":600}""");
        var t = FlowFixtures.temperature(1, 28, h.clock.instant());

        h.send(t);

        List<InMemoryExecutionStore.Outbox> rows = h.store.outbox();
        assertThat(rows).singleElement().satisfies(row -> {
            ActionRequest req = CODEC.read(row.action().payload().toString().getBytes(), ActionRequest.class);
            assertThat(req.idempotencyKey()).isEqualTo(ActionIdempotencyKeys.flow(FlowFixtures.FLOW.toString(), NODE,
                    t.messageId().toString()));
            assertThat(req.priority().name()).isEqualTo("AUTO");
            assertThat(req.validUntil()).isEqualTo(h.clock.instant().plus(Duration.ofSeconds(600)));
            assertThat(req.source().flowVersion()).isEqualTo(1);
            assertThat(req.routingKey()).isEqualTo("command");
            assertThat(req.commandPayload().target().spaceId()).isEqualTo(31L);
            assertThat(req.commandPayload().args()).containsEntry("mode", "cool");
            MessageSchemas.assertValid(req);
        });
    }

    @Test
    @DisplayName("[FLW-05.01] TC-FLW-053 기기 목록 대상은 기기별 1건(분할 인덱스 멱등 키), 같은 메시지를 다시 처리해도 아웃박스는 그대로(BR-FLW-13)")
    void deviceTargetsAndIdempotency() {
        FlowTestHarness h = FlowTestHarness.node("action.control", """
                {"target":{"deviceIds":["41",42]},"capability":"Switch","command":"set","args":{"on":true}}""");
        var t = FlowFixtures.temperature(1, 28, h.clock.instant());

        h.send(t);
        h.send(t);

        assertThat(h.store.outbox()).hasSize(2).extracting(o -> o.action().idempotencyKey()).containsExactly(
                ActionIdempotencyKeys.flow(FlowFixtures.FLOW.toString(), NODE, t.messageId().toString(), 0),
                ActionIdempotencyKeys.flow(FlowFixtures.FLOW.toString(), NODE, t.messageId().toString(), 1));
    }

    @Test
    @DisplayName("[FLW-01.02] BR-FLW-37 priority는 AUTO만, 기능 스키마 밖 인자(5~35℃ 밖)·없는 명령·대상 없음은 컴파일 오류")
    void compileValidation() {
        assertThatThrownBy(() -> FlowTestHarness.node("action.control", """
                {"target":{"deviceId":1},"capability":"Thermostat","command":"set","args":{"targetTemperature":24},"priority":"SAFETY"}"""))
                .hasMessageContaining("config.priority");
        assertThatThrownBy(() -> FlowTestHarness.node("action.control", """
                {"target":{"deviceId":1},"capability":"Thermostat","command":"set","args":{"targetTemperature":40}}"""))
                .hasMessageContaining("config.args.targetTemperature");
        assertThatThrownBy(() -> FlowTestHarness.node("action.control", """
                {"target":{"deviceId":1},"capability":"Thermostat","command":"blast","args":{}}"""))
                .hasMessageContaining("config.command");
        assertThatThrownBy(() -> FlowTestHarness.node("action.control", """
                {"capability":"Switch","command":"set","args":{"on":true}}""")).hasMessageContaining("config.target");
    }

    @Test
    @DisplayName("[FLW-05.01] TC-ACT-027 공유 픽스처 flow-command-heatwave와 같은 모양(kind·출처·대상·명령)")
    void matchesSharedFixture() {
        ActionRequest fixture = MessageFixtures.actionRequest("flow-command-heatwave");
        FlowTestHarness h = FlowTestHarness.node("action.control", """
                {"target":{"spaceId":%d,"relation":"controls","capability":"Thermostat"},"capability":"Thermostat","command":"set",
                 "args":{"mode":"cool","targetTemperature":24}}""".formatted(fixture.commandPayload().target().spaceId()));

        h.send(FlowFixtures.temperature(1, 28, Instant.parse("2026-03-02T00:00:00Z")));

        ActionRequest mine = CODEC.read(h.store.outbox().getFirst().action().payload().toString().getBytes(), ActionRequest.class);
        assertThat(mine.kind()).isEqualTo(fixture.kind());
        assertThat(mine.source().type()).isEqualTo(fixture.source().type());
        assertThat(mine.priority()).isEqualTo(fixture.priority());
        assertThat(mine.commandPayload().capability()).isEqualTo(fixture.commandPayload().capability());
        assertThat(mine.commandPayload().command()).isEqualTo(fixture.commandPayload().command());
        assertThat(mine.commandPayload().target().relation()).isEqualTo(fixture.commandPayload().target().relation());
    }
}
