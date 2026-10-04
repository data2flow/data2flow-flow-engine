package net.java21.data2flow.flow.node;

import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.AlarmSignal;
import net.java21.data2flow.contracts.message.event.EmergencyStopChanged;
import net.java21.data2flow.contracts.message.event.MaintenanceChanged;
import net.java21.data2flow.contracts.notification.NotificationRequest;
import net.java21.data2flow.contracts.sink.SinkMode;
import net.java21.data2flow.contracts.sink.SinkWriteRequest;
import net.java21.data2flow.flow.definition.service.CoreFlowDirectory;
import net.java21.data2flow.flow.guard.service.AutomationGuard;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.Overlay;
import net.java21.data2flow.flow.plan.domain.TimerKind;
import net.java21.data2flow.flow.runtime.domain.InMemoryExecutionStore;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import net.java21.data2flow.flow.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static net.java21.data2flow.flow.support.FlowTestHarness.actions;
import static net.java21.data2flow.flow.support.FlowTestHarness.outputs;
import static net.java21.data2flow.flow.support.FlowTestHarness.ports;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/** 행동 노드(FLW-02·FLW-04.02·RUL-01.01·BR-FLW-10·BR-FLW-19): 제어 결과 포트·비상 정지·바이패스·Sink·알림·알람 */
class ActionNodesTest {

    private static final MessageCodec CODEC = MessageCodec.create();

    private static FlowTestHarness controlFlow(boolean wired) {
        return FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"deviceId":"77"},"capability":"Switch","command":"set","args":{"on":true},"validitySeconds":60}},
                  {"id":"n-ok000001","type":"debug.log","config":{}},
                  {"id":"n-fail0001","type":"debug.log","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-act00001"}%s]}""".formatted(wired
                ? ",{\"from\":\"n-act00001\",\"port\":\"ok\",\"to\":\"n-ok000001\"},{\"from\":\"n-act00001\",\"port\":\"failed\",\"to\":\"n-fail0001\"}"
                : "")));
    }

    private static ActionRequest request(InMemoryExecutionStore.Outbox row) {
        return CODEC.read(row.action().payload().toString(), ActionRequest.class);
    }

    @Test
    @DisplayName("[FLW-02][FLW-05.01] TC-FLW-053 제어 노드 ok·failed 연결: awaitResult=true와 결과 대기 타이머, APPLIED → ok, FAILED → failed(결과 포함)")
    void controlResultPorts() {
        FlowTestHarness h = controlFlow(true);
        var t = FlowFixtures.temperature(1, 28, h.clock.instant());
        h.send(t);

        assertThat(request(h.store.outbox().getFirst()).commandPayload().awaitResult()).isTrue();
        InMemoryExecutionStore.Timer timer = h.store.timers().values().iterator().next();
        assertThat(timer.kind()).isEqualTo(TimerKind.AWAIT_RESULT);
        assertThat(timer.dueAt()).isEqualTo(h.clock.instant().plusSeconds(60 + 60));
        String key = ActionIdempotencyKeys.flow(FlowFixtures.FLOW.toString(), "n-act00001", t.messageId().toString());
        assertThat(timer.context().path("awaitKey").asString()).isEqualTo(key);

        var ok = h.resume(timer.id(), Jsons.object().put("status", "APPLIED").put("commandId", "c-1"));
        assertThat(ports(List.of(ok), "n-act00001")).containsExactly("ok");
        assertThat(outputs(List.of(ok), "n-act00001").getFirst().payload().path("result").path("status").asString()).isEqualTo("APPLIED");
        assertThat(ports(List.of(ok), "n-ok000001")).containsExactly("out");

        h.send(FlowFixtures.temperature(1, 29, h.clock.instant()));
        long second = h.store.timers().keySet().iterator().next();
        var failed = h.resume(second, Jsons.object().put("status", "BLOCKED").put("reason", "INTERLOCK"));
        assertThat(ports(List.of(failed), "n-act00001")).containsExactly("failed");
        assertThat(ports(List.of(failed), "n-fail0001")).containsExactly("out");
    }

    @Test
    @DisplayName("[FLW-02] 제어 결과가 오지 않으면 유효 시각 + 60초에 failed(TIMEOUT), 연결선이 없으면 기다리지 않음(awaitResult=false)")
    void controlTimeoutAndUnwired() {
        FlowTestHarness h = controlFlow(true);
        h.send(FlowFixtures.temperature(1, 28, h.clock.instant()));
        var fired = h.advance(Duration.ofSeconds(120));
        assertThat(ports(fired, "n-act00001")).containsExactly("failed");
        assertThat(outputs(fired, "n-act00001").getFirst().payload().path("result").path("status").asString()).isEqualTo("TIMEOUT");

        FlowTestHarness plain = controlFlow(false);
        plain.send(FlowFixtures.temperature(1, 28, plain.clock.instant()));
        assertThat(request(plain.store.outbox().getFirst()).commandPayload().awaitResult()).isFalse();
        assertThat(plain.store.timers()).isEmpty();
    }

    @Test
    @DisplayName("[FLW-03.05] BR-FLW-11 드라이런: 제어는 기록만(아웃박스 0), ok 포트로 이어서 끝까지 보인다(result DRY_RUN)")
    void controlDryRun() {
        FlowTestHarness h = controlFlow(true).dryRun(true);
        var r = h.send(FlowFixtures.temperature(1, 28, h.clock.instant()));
        assertThat(h.store.outbox()).isEmpty();
        assertThat(actions(r)).singleElement().satisfies(a -> {
            assertThat(a.dryRun()).isTrue();
            assertThat(a.summary()).contains("Switch.set");
        });
        assertThat(ports(r, "n-ok000001")).containsExactly("out");
    }

    @Test
    @DisplayName("[ACT-06.03] TC-FLW-105 BR-FLW-19 비상 정지(조직·공간 범위) 중 제어 노드는 skipped(EMERGENCY_STOP)를 기록하고 실행하지 않음, 해제 뒤 실행")
    void emergencyStop() {
        CoreFlowDirectory core = mock(CoreFlowDirectory.class);
        given(core.spaceDevices(1, 7, true)).willReturn(Set.of(1L, 2L));
        AutomationGuard guard = new AutomationGuard(core);
        FlowTestHarness h = controlFlow(true).guard(guard);

        guard.emergencyStarted(1, new EmergencyStopChanged(9, EmergencyStopChanged.Scope.space(7), "점검", 5L, MutableClock.T0));
        var skipped = h.send(FlowFixtures.telemetry(2, 99L, "temperature", 28, h.clock.instant()));   // 기기 2가 공간 7 아래
        assertThat(h.store.outbox()).isEmpty();
        assertThat(h.store.timers()).as("결과도 기다리지 않음").isEmpty();
        assertThat(actions(skipped)).singleElement().satisfies(a -> assertThat(a.skipped()).isEqualTo("EMERGENCY_STOP"));

        var outside = h.send(FlowFixtures.telemetry(5, 98L, "temperature", 28, h.clock.instant()));
        assertThat(actions(outside)).singleElement().satisfies(a -> assertThat(a.skipped()).as("범위 밖").isNull());

        guard.emergencyReleased(new EmergencyStopChanged(9, EmergencyStopChanged.Scope.space(7), null, 5L, MutableClock.T0));
        guard.emergencyStarted(1, new EmergencyStopChanged(10, EmergencyStopChanged.Scope.organization(), null, 5L, MutableClock.T0));
        assertThat(guard.activeStops()).isEqualTo(1);
        var org = h.send(FlowFixtures.telemetry(6, 97L, "temperature", 28, h.clock.instant()));
        assertThat(actions(org)).singleElement().satisfies(a -> assertThat(a.skipped()).isEqualTo("EMERGENCY_STOP"));
        guard.emergencyReleased(new EmergencyStopChanged(10, EmergencyStopChanged.Scope.organization(), null, 5L, MutableClock.T0));
        var after = h.send(FlowFixtures.telemetry(6, 97L, "temperature", 28, h.clock.instant()));
        assertThat(actions(after)).singleElement().satisfies(a -> assertThat(a.skipped()).isNull());
    }

    @Test
    @DisplayName("[OPS-05.02] 유지보수 창(자동 제어 정지)이 덮는 공간의 제어는 skipped(MAINTENANCE), 알림 노드는 막지 않음")
    void maintenance() {
        AutomationGuard guard = new AutomationGuard(mock(CoreFlowDirectory.class));
        guard.maintenanceStarted(new MaintenanceChanged(3, MaintenanceChanged.TargetType.SPACE, 30, List.of(31L), true, false,
                MutableClock.T0, null));
        FlowTestHarness h = controlFlow(false).guard(guard);
        var r = h.send(FlowFixtures.temperature(1, 28, h.clock.instant()));     // 공간 31
        assertThat(actions(r)).singleElement().satisfies(a -> assertThat(a.skipped()).isEqualTo("MAINTENANCE"));
        guard.maintenanceEnded(new MaintenanceChanged(3, MaintenanceChanged.TargetType.SPACE, 30, List.of(31L), true, false,
                MutableClock.T0, null));
        assertThat(actions(h.send(FlowFixtures.temperature(1, 28, h.clock.instant())))).singleElement()
                .satisfies(a -> assertThat(a.skipped()).isNull());
    }

    @Test
    @DisplayName("[FLW-06.04][AT-FLW-06.1] TC-FLW-145 BR-FLW-10 바이패스: 행동 노드는 실행하지 않고 bypassed 기록(아웃박스 0), 조건 노드는 입력을 첫 포트로 넘김")
    void bypass() {
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"temperature","op":">","value":99}},
                  {"id":"n-act00001","type":"action.notify","config":{"channels":["TELEGRAM"]}}],
                 "wires":[{"from":"n-trg00001","to":"n-thr00001"},{"from":"n-thr00001","port":"true","to":"n-act00001"}]}"""))
                .overlay(new Overlay(Set.of("n-thr00001", "n-act00001"), Set.of(), 3));

        var r = h.send(FlowFixtures.temperature(1, 20, h.clock.instant()));

        assertThat(ports(r, "n-thr00001")).as("바이패스된 조건은 첫 포트(true)").containsExactly("true");
        assertThat(actions(r)).singleElement().satisfies(a -> {
            assertThat(a.skipped()).isEqualTo("bypassed");
            assertThat(a.kind()).isEqualTo("NOTIFY");
        });
        assertThat(h.store.outbox()).isEmpty();
        assertThat(r.getFirst().version()).as("버전을 올리지 않음").isEqualTo(1);
    }

    @Test
    @DisplayName("[FLW-04.02] TC-FLW-058 sink.database: 매핑·upsert(같은 키는 1행)·배치 100건 → SinkWriteRequest, 멱등 키는 배치 인덱스 포함, ok 포트")
    void sink() {
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-snk00001","type":"sink.database","config":{"connectionId":"4","target":"room_temp","mode":"upsert","upsertKeys":["device_id","ts"],
                     "mapping":[{"field":"$.deviceId","column":"device_id"},{"field":"ts","column":"ts"},{"field":"temp","column":"temperature"}]}},
                  {"id":"n-dbg00001","type":"debug.log","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-snk00001"},{"from":"n-snk00001","port":"ok","to":"n-dbg00001"}]}"""));
        // 앞단 JS 함수가 돌려준 레코드 배열(251건, 마지막은 t0과 같은 키)
        StringBuilder records = new StringBuilder("[");
        for (int i = 0; i < 250; i++) {
            records.append("{\"ts\":\"t").append(i).append("\",\"temp\":").append(i).append("},");
        }
        records.append("{\"ts\":\"t0\",\"temp\":999}]");
        String messageId = UUID.randomUUID().toString();
        var r = List.of(h.sendTo("n-snk00001", "{\"messageId\":\"" + messageId + "\",\"deviceId\":15,\"payload\":" + records + "}"));

        List<InMemoryExecutionStore.Outbox> rows = h.store.outbox();
        assertThat(rows).hasSize(3);
        List<SinkWriteRequest> batches = rows.stream().map(o -> Jsons.MAPPER.treeToValue(request(o).payload(), SinkWriteRequest.class))
                .toList();
        assertThat(batches).extracting(SinkWriteRequest::batchIndex).containsExactly(0, 1, 2);
        assertThat(batches.stream().mapToInt(b -> b.records().size()).sum()).as("같은 키(t0) 두 건은 한 행").isEqualTo(250);
        assertThat(batches.getFirst().mode()).isEqualTo(SinkMode.UPSERT);
        assertThat(batches.stream().flatMap(b -> b.records().stream()).filter(x -> "t0".equals(x.get("ts"))).toList())
                .singleElement().satisfies(x -> assertThat(((Number) x.get("temperature")).intValue()).isEqualTo(999));
        assertThat(((Number) batches.getFirst().records().getFirst().get("device_id")).longValue()).isEqualTo(15L);
        assertThat(rows.getFirst().action().routingKey()).isEqualTo("sink");
        assertThat(rows.get(1).action().idempotencyKey())
                .isEqualTo(ActionIdempotencyKeys.flow(FlowFixtures.FLOW.toString(), "n-snk00001", messageId, 1));
        assertThat(outputs(r, "n-snk00001").getFirst().payload().path("sink").path("records").asInt()).isEqualTo(250);
        assertThat(ports(r, "n-dbg00001")).containsExactly("out");
    }

    @Test
    @DisplayName("[FLW-04.02] Sink 레코드가 없거나 UPSERT 키 열이 빠지면 failed 포트(연결 없으면 error), 매핑이 없으면 원시값 필드 그대로")
    void sinkFailures() {
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                  {"id":"n-snk00001","type":"sink.database","config":{"connectionId":4,"target":"t","mode":"upsert","upsertKeys":["missing"]}},
                  {"id":"n-snk00002","type":"sink.database","config":{"connectionId":4,"target":"t"}},
                  {"id":"n-dbg00001","type":"debug.log","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-snk00001"},{"from":"n-trg00001","to":"n-snk00002"},{"from":"n-snk00001","port":"failed","to":"n-dbg00001"}]}"""));

        var r = h.send(FlowFixtures.temperature(1, 28, h.clock.instant()));

        assertThat(ports(r, "n-snk00001")).containsExactly("failed");
        assertThat(outputs(r, "n-snk00001").getFirst().payload().path("sink").path("error").asString()).contains("missing");
        assertThat(ports(r, "n-snk00002")).containsExactly("ok");
        SinkWriteRequest plain = Jsons.MAPPER.treeToValue(request(h.store.outbox().getFirst()).payload(), SinkWriteRequest.class);
        assertThat(plain.records()).singleElement().satisfies(rec -> assertThat(rec).containsEntry("temperature", 28.0));
        assertThat(plain.batchIndex()).isNull();
    }

    @Test
    @DisplayName("[FLW-02] TC-FLW-055 action.notify: 정책 → NotificationRequest(템플릿·변수·묶기 키), 채널을 직접 지정하면 정책 무시")
    void notifyNode() {
        FlowTestHarness h = FlowTestHarness.node("action.notify",
                "{\"policyId\":\"7\",\"templateKey\":\"cooling.no-effect\",\"aggregateWindow\":\"PT2M\",\"severity\":\"MAJOR\","
                        + "\"variables\":{\"temp\":\"payload.temperature\"}}");
        var t = FlowFixtures.temperature(1, 28.5, h.clock.instant());
        h.send(t);
        ActionRequest a = request(h.store.outbox().getFirst());
        NotificationRequest n = Jsons.MAPPER.treeToValue(a.payload(), NotificationRequest.class);
        assertThat(a.routingKey()).isEqualTo("notify");
        assertThat(n.policyId()).isEqualTo(7L);
        assertThat(n.needsPolicyResolution()).isTrue();
        assertThat(n.templateKeyFor("TELEGRAM")).isEqualTo("cooling.no-effect");
        assertThat(n.aggregateWindowSec()).isEqualTo(120);
        assertThat(n.aggregateKey()).isEqualTo("flow:" + FlowFixtures.FLOW + ":n-test0001:device:1");
        assertThat(n.variables()).containsEntry("temp", 28.5).containsEntry("deviceId", "1");
        assertThat(n.event()).isEqualTo("flow.notify");

        FlowTestHarness direct = FlowTestHarness.node("action.notify", "{\"policyId\":\"7\",\"channels\":[\"telegram\"]}");
        direct.send(FlowFixtures.temperature(1, 28.5, direct.clock.instant()));
        NotificationRequest d = Jsons.MAPPER.treeToValue(request(direct.store.outbox().getFirst()).payload(), NotificationRequest.class);
        assertThat(d.policyId()).isNull();
        assertThat(d.recipients()).singleElement().satisfies(rc -> assertThat(rc.channel()).isEqualTo("TELEGRAM"));
    }

    @Test
    @DisplayName("[FLW-02][RUL-01.01] TC-FLW-054·TC-RUL-038 action.alarm: raise → alarm.signal RAISE(키 flow:{flowId}:{nodeId}:{기기}), clear → CLEAR, 규칙이면 rule:{ruleId}:{대상}")
    void alarmNode() {
        FlowTestHarness h = FlowTestHarness.node("action.alarm",
                "{\"mode\":\"raise\",\"severity\":\"MAJOR\",\"title\":\"고온 {{payload.temperature}}℃\",\"metric\":\"temperature\","
                        + "\"threshold\":{\"raise\":27,\"clear\":26}}");
        var t = FlowFixtures.temperature(15, 28.5, h.clock.instant());
        h.send(t);

        var row = h.store.outbox().getFirst();
        assertThat(row.action().kind()).isEqualTo("EVENT");
        assertThat(row.action().exchange()).isEqualTo("data2flow.events");
        assertThat(row.action().routingKey()).isEqualTo("alarm.signal");
        AlarmSignal s = CODEC.readEvent(row.action().payload().toString().getBytes(), AlarmSignal.class).payload();
        assertThat(s.signal()).isEqualTo(AlarmSignal.Signal.RAISE);
        assertThat(s.alarmKey()).isEqualTo("flow:" + FlowFixtures.FLOW + ":n-test0001:15");
        assertThat(s.title()).isEqualTo("고온 28.5℃");
        assertThat(s.value()).isEqualTo(28.5);
        assertThat(s.threshold().clear()).isEqualTo(26);
        assertThat(s.triggerMessageId()).isEqualTo(t.messageId().toString());

        FlowTestHarness rule = FlowTestHarness.node("action.alarm", "{\"mode\":\"clear\",\"ruleId\":\"42\"}");
        rule.sendTo("n-test0001", "{\"payload\":{\"temperature\":20},\"measuredAt\":\"2026-03-02T00:00:00Z\"}".replace("{\"payload",
                "{\"spaceId\":31,\"payload"));
        AlarmSignal c = CODEC.readEvent(rule.store.outbox().getFirst().action().payload().toString().getBytes(), AlarmSignal.class)
                .payload();
        assertThat(c.signal()).isEqualTo(AlarmSignal.Signal.CLEAR);
        assertThat(c.alarmKey()).isEqualTo("rule:42:space-31");
        assertThat(c.ruleId()).isEqualTo(42L);
    }

    @Test
    @DisplayName("[FLW-02] action.alarm 대상(기기·공간)을 찾지 못하면 error(ALARM_TARGET_MISSING), 대상 키가 공간이면 공간 알람")
    void alarmTargets() {
        FlowTestHarness h = FlowTestHarness.node("action.alarm", "{\"mode\":\"raise\",\"severity\":\"MINOR\",\"title\":\"x\"}");
        var r = h.sendTo("n-test0001", "{\"payload\":{}}".replace("{\"payload", "{\"x\":1,\"payload"));
        assertThat(r.steps().stream().filter(s -> "ALARM_TARGET_MISSING".equals(s.errorType()))).isEmpty();
        // sendTo의 대상 키는 device:1 → 기기 1 알람
        AlarmSignal s = CODEC.readEvent(h.store.outbox().getFirst().action().payload().toString().getBytes(), AlarmSignal.class)
                .payload();
        assertThat(s.deviceId()).isEqualTo(1L);
        assertThat(UUID.fromString(s.flowId())).isEqualTo(FlowFixtures.FLOW);
        Instant measured = s.measuredAt();
        assertThat(measured).isEqualTo(h.clock.instant());
    }
}
