package net.java21.data2flow.flow.apply.service;

import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.flow.definition.service.FlowSynchronizer;
import net.java21.data2flow.flow.guard.service.AutomationGuard;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;

import java.util.UUID;

/**
 * 설정 변경 수신(EVT-FLW-04·EVT-DEV-04, architecture.md §4.5): fanout {@code data2flow.config}를 인스턴스별 임시 큐
 * ({@code flow.config.<난수>})로 받는다. 봉투는 공통 {@link ConfigChangedMessage}이고 내용이 아니라 종류·ID만 보고 원천(core 내부 API)에서
 * 다시 읽는다.
 * <table>
 *   <tr><th>entityType</th><th>동작</th></tr>
 *   <tr><td>FLOW·OVERLAY (id = flowId)</td><td>API-FLW-81로 다시 읽어 적용(라이브 리로드, 1초 안 반영)</td></tr>
 *   <tr><td>DEVICE·SPACE·GROUP·MODEL</td><td>공간 측정 기기 캐시를 다시 읽게 한다(트리거 대상)</td></tr>
 *   <tr><td>EMERGENCY_STOP</td><td>비상 정지 목록을 다시 읽는다(BR-FLW-19, 1초 안 반영)</td></tr>
 *   <tr><td>VARIABLE·SUBFLOW</td><td>아직 받지 않음(플로우 변수·서브플로우 미구현)</td></tr>
 * </table>
 * 연결이 다시 맺어지면 놓친 메시지가 있을 수 있으므로 전체를 다시 읽는다({@link #resync}).
 */
public class ConfigChangeListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(ConfigChangeListener.class);

    private final MessageCodec codec = MessageCodec.create();
    private final FlowSynchronizer synchronizer;
    private final SpaceDirectory spaces;
    private final AutomationGuard guard;

    public ConfigChangeListener(FlowSynchronizer synchronizer, SpaceDirectory spaces, AutomationGuard guard) {
        this.synchronizer = synchronizer;
        this.spaces = spaces;
        this.guard = guard;
    }

    @Override
    public void onMessage(Message message) {
        ConfigChangedMessage change;
        try {
            change = codec.read(message.getBody(), ConfigChangedMessage.class);
        } catch (RuntimeException e) {
            log.warn("읽을 수 없는 설정 변경 메시지를 무시합니다: {}", e.getMessage());
            return;
        }
        apply(change);
    }

    public void apply(ConfigChangedMessage change) {
        switch (change.entityType()) {
            case FLOW, OVERLAY -> {
                UUID flowId;
                try {
                    flowId = UUID.fromString(change.id());
                } catch (IllegalArgumentException e) {
                    log.warn("플로우 ID가 UUID가 아닙니다: {}", change.id());
                    return;
                }
                try {
                    synchronizer.syncFlow(flowId);
                } catch (RuntimeException e) {
                    log.warn("플로우 {} 다시 읽기 실패(주기 동기화로 따라잡음): {}", flowId, e.getMessage());
                }
            }
            case DEVICE, SPACE, GROUP, MODEL -> spaces.invalidateAll();
            case EMERGENCY_STOP -> {
                if (guard != null) {
                    guard.refresh();
                }
            }
            default -> {
                // 다른 서비스용·M4 종류는 무시
            }
        }
    }

    /** 재연결: 전체 다시 읽기 */
    public void resync() {
        spaces.invalidateAll();
        if (guard != null) {
            guard.refresh();
        }
        try {
            synchronizer.syncAll(true);
        } catch (RuntimeException e) {
            log.warn("전체 동기화 실패(주기 동기화로 따라잡음): {}", e.getMessage());
        }
    }
}
