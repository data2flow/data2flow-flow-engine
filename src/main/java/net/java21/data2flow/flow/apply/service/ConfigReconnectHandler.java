package net.java21.data2flow.flow.apply.service;

import org.springframework.amqp.rabbit.listener.AsyncConsumerStartedEvent;
import org.springframework.context.event.EventListener;

/**
 * 설정 변경 큐 소비자가 (다시) 시작되면 놓친 메시지가 있을 수 있으므로 전체를 다시 읽는다(live-reload §1 ⑤ "재연결하면 DB에서 다시 읽어
 * 놓친 이벤트를 보완"). 처음 시작은 주기 동기화가 이미 하므로 건너뛴다.
 */
public class ConfigReconnectHandler {

    private final ConfigChangeListener listener;
    private volatile boolean started;

    public ConfigReconnectHandler(ConfigChangeListener listener) {
        this.listener = listener;
    }

    @EventListener
    public void onConsumerStarted(AsyncConsumerStartedEvent event) {
        if (started) {
            listener.resync();
        }
        started = true;
    }
}
