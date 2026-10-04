package net.java21.data2flow.flow.runtime;

import com.rabbitmq.client.AMQP;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.AlarmSignal;
import net.java21.data2flow.contracts.message.event.CommandStatusChanged;
import net.java21.data2flow.contracts.message.event.EmergencyStopChanged;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.QuorumQueueSpec;
import net.java21.data2flow.contracts.notification.NotificationRequest;
import net.java21.data2flow.contracts.sink.SinkWriteRequest;
import net.java21.data2flow.flow.guard.service.AutomationGuard;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.IntegrationTestSupport;
import net.java21.data2flow.flow.support.TestInfrastructure;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 자동화 완성 통합 시험(Testcontainers PostgreSQL 18 + RabbitMQ 3.13): 제어 결과 포트(EVT-ACT-01), 비상 정지(EVT-ACT-03), 알림·Sink 행동 요청 큐,
 * 규칙 컴파일 → 알람 신호(EVT-RUL-01). 플로우마다 다른 기기를 대상으로 해서 시험끼리 섞이지 않는다.
 */
class AutomationIT extends IntegrationTestSupport {

    private static final MessageCodec CODEC = MessageCodec.create();
    private static final Clock WALL = Clock.systemUTC();

    @Autowired
    FlowRegistry registry;
    @Autowired
    JdbcClient jdbc;
    @Autowired
    AutomationGuard guard;
    @LocalServerPort
    int port;

    private void deploy(UUID flow, int version, FlowDefinition definition) {
        TestInfrastructure.CORE.put(flow, FlowFixtures.ORG, version, "ACTIVE", definition);
        await().atMost(Duration.ofSeconds(20)).until(() -> registry.get(flow).map(f -> f.version() == version).orElse(false));
    }

    private static CanonicalTelemetry reading(long device, String metric, double value) {
        Instant now = Instant.now(WALL);
        return CanonicalTelemetry.builder().organizationId(FlowFixtures.ORG).sourceId(3).externalId("auto-" + device).deviceId(device)
                .modelId("auto").spaceId(9000L + device).measuredAt(now).receivedAt(now)
                .metric(CanonicalTelemetry.Metric.of(metric, value, null)).rawMessageId(1).build();
    }

    private static void publishEvent(DomainEvent<?> event) throws Exception {
        STREAMS.channel().basicPublish(MessagingNames.EXCHANGE_EVENTS, event.type(),
                new AMQP.BasicProperties.Builder().contentType("application/json").build(), CODEC.write(event));
    }

    private static List<ActionRequest> drainActions(String queue, UUID flow) {
        List<ActionRequest> out = new ArrayList<>();
        for (byte[] body : STREAMS.drain(queue)) {
            ActionRequest a = CODEC.read(body, ActionRequest.class);
            if (flow.toString().equals(a.source().flowId())) {
                out.add(a);
            }
        }
        return out;
    }

    @Test
    @DisplayName("[FLW-02][FLW-05.01] TC-FLW-053 제어 결과 포트: awaitResult 명령 → EVT-ACT-01 APPLIED(출처 FLOW, 같은 멱등 키) → ok 포트 → 알림 요청이 action.notifications로")
    void controlResultContinuesFlow() throws Exception {
        UUID flow = UUID.randomUUID();
        deploy(flow, 1, FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["7101"]}}},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"deviceId":"7102"},"capability":"Switch","command":"set","args":{"on":true}}},
                  {"id":"n-ntf00001","type":"action.notify","config":{"channels":["TELEGRAM"],"templateKey":"cooling.applied"}}],
                 "wires":[{"from":"n-trg00001","to":"n-act00001"},{"from":"n-act00001","port":"ok","to":"n-ntf00001"}]}"""));
        CanonicalTelemetry t = reading(7101, "temperature", 30);
        STREAMS.publish(t);
        await().atMost(Duration.ofSeconds(20)).until(() -> STREAMS.commands(flow).size() == 1);
        ActionRequest command = STREAMS.commands(flow).getFirst();
        assertThat(command.commandPayload().awaitResult()).isTrue();
        assertThat(command.idempotencyKey()).isEqualTo(ActionIdempotencyKeys.flow(flow.toString(), "n-act00001", t.messageId().toString()));

        publishEvent(DomainEvent.of(EventType.commandStatus(CommandStatus.APPLIED), FlowFixtures.ORG, new CommandStatusChanged(
                UUID.randomUUID(), command.idempotencyKey(), 7102, null, "Switch", "set", Map.of("on", true), CommandStatus.APPLIED, null,
                null, command.source(), CommandPriority.AUTO, Instant.now(WALL)), null, WALL));

        List<ActionRequest> notifications = new ArrayList<>();
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            notifications.addAll(drainActions(QuorumQueueSpec.ACTION_NOTIFICATIONS.name(), flow));
            return !notifications.isEmpty();
        });
        NotificationRequest n = Jsons.MAPPER.treeToValue(notifications.getFirst().payload(), NotificationRequest.class);
        assertThat(n.templateKeyFor("TELEGRAM")).isEqualTo("cooling.applied");
        assertThat(jdbc.sql("SELECT status FROM data2flow_flow.flow_timers WHERE flow_id = :f").param("f", flow).query(String.class)
                .list()).containsExactly("FIRED");
    }

    @Test
    @DisplayName("[ACT-06.03][FLW-04.02] BR-FLW-19 비상 정지(EVT-ACT-03) 중 제어는 건너뛰고 Sink는 action.sinks로, 해제(EVT-ACT-03 released) 뒤 제어 재개")
    void emergencyStopAndSink() throws Exception {
        UUID flow = UUID.randomUUID();
        deploy(flow, 1, FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["7201"]}}},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"deviceId":"7202"},"capability":"Switch","command":"set","args":{"on":true}}},
                  {"id":"n-snk00001","type":"sink.database","config":{"connectionId":"4","target":"room_temp","mapping":[{"field":"$.deviceId","column":"device_id"},{"field":"temperature","column":"temperature"}]}}],
                 "wires":[{"from":"n-trg00001","to":"n-act00001"},{"from":"n-trg00001","to":"n-snk00001"}]}"""));
        EmergencyStopChanged stop = new EmergencyStopChanged(77, EmergencyStopChanged.Scope.organization(), "점검", 5L, Instant.now(WALL));
        publishEvent(DomainEvent.of(EventType.CONTROL_EMERGENCY_STARTED, FlowFixtures.ORG, stop, null, WALL));
        await().atMost(Duration.ofSeconds(10)).until(() -> guard.activeStops() == 1);

        STREAMS.publish(reading(7201, "temperature", 30));
        List<ActionRequest> sinks = new ArrayList<>();
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            sinks.addAll(drainActions(QuorumQueueSpec.ACTION_SINKS.name(), flow));
            return !sinks.isEmpty();
        });
        SinkWriteRequest sink = Jsons.MAPPER.treeToValue(sinks.getFirst().payload(), SinkWriteRequest.class);
        assertThat(sink.records()).singleElement().satisfies(r -> assertThat(((Number) r.get("device_id")).longValue()).isEqualTo(7201));
        assertThat(STREAMS.commands(flow)).as("비상 정지 중 제어 0건").isEmpty();

        publishEvent(DomainEvent.of(EventType.CONTROL_EMERGENCY_RELEASED, FlowFixtures.ORG, stop, null, WALL));
        await().atMost(Duration.ofSeconds(10)).until(() -> guard.activeStops() == 0);
        STREAMS.publish(reading(7201, "temperature", 31));
        await().atMost(Duration.ofSeconds(20)).until(() -> STREAMS.commands(flow).size() == 1);
    }

    @Test
    @DisplayName("[RUL-01.01][AT-RUL-02.2] TC-RUL-006·009·010 규칙 컴파일(내부 API) → 플로우 배포 → 1,050ppm 지속 → alarm.signal RAISE, 950 유지(히스테리시스), 880 → CLEAR")
    void ruleToAlarmSignals() throws Exception {
        STREAMS.bindEvents("it.flow.alarms", "alarm.signal");
        STREAMS.drain("it.flow.alarms");
        HttpResponse<String> compiled = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/internal/flow/rules/compile"))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString("""
                        {"organizationId":"1","ruleId":"501","rule":{"scope":{"type":"DEVICE","ids":["7301"]},
                         "condition":{"kind":"threshold","metric":"co2","op":">","value":1000,"for":"PT2S","clear":900},
                         "severity":"MAJOR","titleTemplate":"고CO2 {{value}}ppm"}}""")).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(compiled.statusCode()).isEqualTo(200);
        FlowDefinition definition = Jsons.MAPPER.treeToValue(Jsons.MAPPER.readTree(compiled.body()).path("response").path("definition"),
                FlowDefinition.class);
        UUID flow = UUID.randomUUID();
        deploy(flow, 1, definition);

        STREAMS.publish(reading(7301, "co2", 1050));
        List<AlarmSignal> signals = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            STREAMS.drain("it.flow.alarms").forEach(b -> signals.add(CODEC.readEvent(b, AlarmSignal.class).payload()));
            return !signals.isEmpty();
        });
        assertThat(signals.getFirst().signal()).isEqualTo(AlarmSignal.Signal.RAISE);
        assertThat(signals.getFirst().alarmKey()).isEqualTo("rule:501:7301");
        assertThat(signals.getFirst().title()).isEqualTo("고CO2 1050.0ppm");
        assertThat(signals.getFirst().value()).isEqualTo(1050);

        STREAMS.publish(reading(7301, "co2", 950));
        STREAMS.publish(reading(7301, "co2", 880));
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            STREAMS.drain("it.flow.alarms").forEach(b -> signals.add(CODEC.readEvent(b, AlarmSignal.class).payload()));
            return signals.size() >= 2;
        });
        assertThat(signals).extracting(AlarmSignal::signal).containsExactly(AlarmSignal.Signal.RAISE, AlarmSignal.Signal.CLEAR);
        assertThat(signals.getLast().value()).isEqualTo(880);
    }
}
