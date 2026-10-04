package net.java21.data2flow.flow.apply.service;

import net.java21.data2flow.contracts.command.SourceType;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageFormatException;
import net.java21.data2flow.contracts.message.event.CommandStatusChanged;
import net.java21.data2flow.contracts.message.event.EmergencyStopChanged;
import net.java21.data2flow.contracts.message.event.EventPayload;
import net.java21.data2flow.contracts.message.event.MaintenanceChanged;
import net.java21.data2flow.flow.guard.service.AutomationGuard;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.runtime.service.FlowRuntimeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import tools.jackson.databind.node.ObjectNode;

/**
 * flow-engine이 받는 도메인 이벤트({@code data2flow.events}).
 *
 * <table>
 *   <tr><th>이벤트</th><th>큐</th><th>동작</th></tr>
 *   <tr><td>EVT-ACT-01 {@code command.status.*}(끝 상태, 출처 FLOW)</td><td>{@code flow.events}(Quorum, 인스턴스들이 나눠 받음)</td>
 *       <td>제어 노드 결과 대기 타이머를 찾아 ok·failed로 이어서 실행({@link FlowRuntimeService#resumeAwaiting}). DB 장애면 예외로 다시 받는다</td></tr>
 *   <tr><td>EVT-ACT-03 {@code control.emergency.started|released}</td><td>인스턴스 임시 큐 {@code flow.guard.*}</td><td>비상 정지 범위 반영(BR-FLW-19)</td></tr>
 *   <tr><td>EVT-OPS-02 {@code ops.maintenance.started|ended}</td><td>인스턴스 임시 큐</td><td>유지보수 자동 제어 정지 반영(OPS-05.02)</td></tr>
 * </table>
 * 읽을 수 없는 메시지·모르는 이벤트는 무시한다(다시 받아도 같으므로).
 */
public class FlowEventListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(FlowEventListener.class);

    private final MessageCodec codec = MessageCodec.create();
    private final FlowRuntimeService runtime;
    private final AutomationGuard guard;

    public FlowEventListener(FlowRuntimeService runtime, AutomationGuard guard) {
        this.runtime = runtime;
        this.guard = guard;
    }

    @Override
    public void onMessage(Message message) {
        DomainEvent<? extends EventPayload> event;
        try {
            event = codec.readEvent(message.getBody());
        } catch (MessageFormatException | IllegalArgumentException e) {
            log.warn("읽을 수 없는 이벤트를 무시합니다: {}", e.getMessage());
            return;
        }
        handle(event);
    }

    public void handle(DomainEvent<? extends EventPayload> event) {
        switch (event.payload()) {
            case CommandStatusChanged c -> {
                if (c.status() == null || !c.status().terminal() || c.source() == null || c.source().type() != SourceType.FLOW
                        || c.idempotencyKey() == null) {
                    return;
                }
                ObjectNode result = Jsons.object().put("status", c.status().name());
                if (c.reason() != null) {
                    result.put("reason", c.reason());
                }
                if (c.message() != null) {
                    result.put("message", c.message());
                }
                if (c.commandId() != null) {
                    result.put("commandId", c.commandId().toString());
                }
                if (c.at() != null) {
                    result.put("at", c.at().toString());
                }
                result.put("deviceId", c.deviceId());
                int resumed = runtime.resumeAwaiting(event.organizationId(), c.idempotencyKey(), result);
                log.debug("명령 결과 {} {} → 이어서 실행 {}건", c.idempotencyKey(), c.status(), resumed);
            }
            case EmergencyStopChanged e -> {
                if (EventType.CONTROL_EMERGENCY_STARTED.routingKey().equals(event.type())) {
                    guard.emergencyStarted(event.organizationId(), e);
                } else {
                    guard.emergencyReleased(e);
                }
            }
            case MaintenanceChanged m -> {
                if (EventType.OPS_MAINTENANCE_STARTED.routingKey().equals(event.type())) {
                    guard.maintenanceStarted(m);
                } else {
                    guard.maintenanceEnded(m);
                }
            }
            default -> {
                // 다른 이벤트는 받지 않는다(바인딩 밖)
            }
        }
    }
}
