package net.java21.data2flow.flow.apply;

import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.CommandStatusChanged;
import net.java21.data2flow.contracts.message.event.EmergencyStopChanged;
import net.java21.data2flow.contracts.message.event.MaintenanceChanged;
import net.java21.data2flow.flow.apply.service.FlowEventListener;
import net.java21.data2flow.flow.guard.service.AutomationGuard;
import net.java21.data2flow.flow.runtime.service.FlowRuntimeService;
import net.java21.data2flow.flow.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import tools.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** 엔진이 받는 도메인 이벤트: EVT-ACT-01 제어 결과(ok·failed 포트), EVT-ACT-03 비상 정지, EVT-OPS-02 유지보수 */
class FlowEventListenerTest {

    private static final MessageCodec CODEC = MessageCodec.create();
    private final MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
    private final FlowRuntimeService runtime = mock(FlowRuntimeService.class);
    private final AutomationGuard guard = mock(AutomationGuard.class);
    private final FlowEventListener listener = new FlowEventListener(runtime, guard);

    private Message message(DomainEvent<?> event) {
        return new Message(CODEC.write(event), new MessageProperties());
    }

    private CommandStatusChanged status(CommandStatus s, CommandSource source) {
        return new CommandStatusChanged(UUID.randomUUID(), "key-1", 77, 31L, "Switch", "set", Map.of("on", true), s, "INTERLOCK",
                "창문 열림", source, CommandPriority.AUTO, clock.instant());
    }

    @Test
    @DisplayName("[FLW-02] EVT-ACT-01 끝 상태(출처 FLOW)는 멱등 키로 결과 대기 타이머를 이어 실행, 중간 상태·다른 출처는 무시")
    void commandResults() {
        CommandSource flow = CommandSource.flow(UUID.randomUUID().toString(), 3, "n-act00001", "m-1");
        listener.onMessage(message(DomainEvent.of(EventType.commandStatus(CommandStatus.BLOCKED), 1, status(CommandStatus.BLOCKED, flow),
                null, clock)));
        ArgumentCaptor<JsonNode> result = ArgumentCaptor.forClass(JsonNode.class);
        verify(runtime).resumeAwaiting(eq(1L), eq("key-1"), result.capture());
        assertThat(result.getValue().path("status").asString()).isEqualTo("BLOCKED");
        assertThat(result.getValue().path("reason").asString()).isEqualTo("INTERLOCK");
        assertThat(result.getValue().path("message").asString()).isEqualTo("창문 열림");

        listener.onMessage(message(DomainEvent.of(EventType.commandStatus(CommandStatus.SENT), 1, status(CommandStatus.SENT, flow),
                null, clock)));
        listener.onMessage(message(DomainEvent.of(EventType.commandStatus(CommandStatus.APPLIED), 1,
                status(CommandStatus.APPLIED, CommandSource.user(5L)), null, clock)));
        verify(runtime, org.mockito.Mockito.times(1)).resumeAwaiting(anyLong(), anyString(), any());
        listener.onMessage(new Message("not json".getBytes(), new MessageProperties()));
    }

    @Test
    @DisplayName("[ACT-06.03][OPS-05.02] EVT-ACT-03·EVT-OPS-02는 자동 제어 차단 상태로 바로 반영")
    void guardEvents() {
        EmergencyStopChanged stop = new EmergencyStopChanged(9, EmergencyStopChanged.Scope.organization(), "점검", 5L, clock.instant());
        listener.onMessage(message(DomainEvent.of(EventType.CONTROL_EMERGENCY_STARTED, 1, stop, null, clock)));
        verify(guard).emergencyStarted(1, stop);
        listener.onMessage(message(DomainEvent.of(EventType.CONTROL_EMERGENCY_RELEASED, 1, stop, null, clock)));
        verify(guard).emergencyReleased(stop);
        MaintenanceChanged m = new MaintenanceChanged(3, MaintenanceChanged.TargetType.DEVICE, 7, List.of(), true, false,
                clock.instant(), null);
        listener.onMessage(message(DomainEvent.of(EventType.OPS_MAINTENANCE_STARTED, 1, m, null, clock)));
        verify(guard).maintenanceStarted(m);
        listener.onMessage(message(DomainEvent.of(EventType.OPS_MAINTENANCE_ENDED, 1, m, null, clock)));
        verify(guard).maintenanceEnded(m);
        verify(runtime, never()).resumeAwaiting(anyLong(), anyString(), any());
    }
}
