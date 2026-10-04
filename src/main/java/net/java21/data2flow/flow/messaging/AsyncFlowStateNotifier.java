package net.java21.data2flow.flow.messaging;

import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.FlowStateChanged;
import net.java21.data2flow.flow.runtime.domain.FlowStateNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * EVT-FLW-03 {@code flow.state.changed} 발행. 판정은 메시지 처리 경로(파티션 작업 스레드)에서 일어나므로 발행 확인을 기다리는 일은 별도
 * 스레드에서 한다. 발행이 실패하면 한 번 더 시도하고 기록만 남긴다(엔진은 이미 멈췄고, core는 다음 판정·주기 동기화로 따라잡는다).
 */
public class AsyncFlowStateNotifier implements FlowStateNotifier, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AsyncFlowStateNotifier.class);

    private final EventPublisher events;
    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon().name("flow-state-notifier").factory());

    public AsyncFlowStateNotifier(EventPublisher events) {
        this.events = events;
    }

    @Override
    public void notify(long organizationId, FlowStateChanged change) {
        executor.execute(() -> {
            for (int attempt = 1; attempt <= 2; attempt++) {
                try {
                    events.publish(EventType.FLOW_STATE_CHANGED, organizationId, change);
                    return;
                } catch (RuntimeException e) {
                    log.warn("flow.state.changed 발행 실패({}회): {}", attempt, e.getMessage());
                }
            }
        });
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
