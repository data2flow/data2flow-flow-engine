package net.java21.data2flow.flow.apply;

import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.flow.apply.service.ConfigChangeListener;
import net.java21.data2flow.flow.apply.service.ConfigReconnectHandler;
import net.java21.data2flow.flow.definition.service.FlowSynchronizer;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/** EVT-FLW-04·EVT-DEV-04 설정 변경 수신(data2flow.config): FLOW·OVERLAY는 그 플로우만 다시 읽고, 기기·공간 변경은 공간 캐시, 재연결은 전체 */
class ConfigChangeListenerTest {

    private final FlowSynchronizer sync = mock(FlowSynchronizer.class);
    private final SpaceDirectory spaces = mock(SpaceDirectory.class);
    private final ConfigChangeListener listener = new ConfigChangeListener(sync, spaces, null);
    private final MessageCodec codec = MessageCodec.create();

    private Message message(ConfigChangedMessage.EntityType type, String id) {
        ConfigChangedMessage m = new ConfigChangedMessage(1, UUID.randomUUID(), type, id, 3, ConfigChangedMessage.Op.UPSERT, "1",
                Instant.parse("2026-03-02T00:00:00Z"));
        return new Message(codec.write(m), new MessageProperties());
    }

    @Test
    @DisplayName("[FLW-01.06] FLOW·OVERLAY 변경은 API-FLW-81로 그 플로우만 다시 읽는다(라이브 리로드)")
    void flowChange() {
        UUID flow = UUID.randomUUID();

        listener.onMessage(message(ConfigChangedMessage.EntityType.FLOW, flow.toString()));
        listener.onMessage(message(ConfigChangedMessage.EntityType.OVERLAY, flow.toString()));
        listener.onMessage(message(ConfigChangedMessage.EntityType.FLOW, "not-a-uuid"));

        verify(sync, times(2)).syncFlow(flow);
    }

    @Test
    @DisplayName("[FLW-05.01] DEVICE·SPACE 변경은 공간 측정 기기 캐시를 다시 읽게 하고, 모르는 종류·깨진 메시지는 무시")
    void deviceChangeAndUnknown() {
        listener.onMessage(message(ConfigChangedMessage.EntityType.SPACE, "31"));
        listener.onMessage(message(ConfigChangedMessage.EntityType.SCRIPT, "9"));
        listener.onMessage(new Message("{".getBytes(), new MessageProperties()));

        verify(spaces).invalidateAll();
        verifyNoInteractions(sync);
    }

    @Test
    @DisplayName("[FLW-01.06] 설정 큐 소비자가 다시 연결되면(처음 시작 제외) 전체 다시 읽기")
    void reconnect() {
        ConfigReconnectHandler handler = new ConfigReconnectHandler(listener);

        handler.onConsumerStarted(null);
        verify(sync, never()).syncAll(true);
        handler.onConsumerStarted(null);

        verify(sync).syncAll(true);
    }
}
