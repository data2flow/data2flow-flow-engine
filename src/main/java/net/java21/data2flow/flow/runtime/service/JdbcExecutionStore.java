package net.java21.data2flow.flow.runtime.service;

import net.java21.data2flow.flow.outbox.repository.OutboxRepository;
import net.java21.data2flow.flow.plan.domain.ActionDraft;
import net.java21.data2flow.flow.plan.domain.TimerKind;
import net.java21.data2flow.flow.runtime.domain.ExecutionStore;
import net.java21.data2flow.flow.runtime.domain.NodeMetrics;
import net.java21.data2flow.flow.runtime.repository.NodeStateRepository;
import net.java21.data2flow.flow.timer.repository.TimerRepository;
import net.java21.data2flow.flow.runtime.domain.StoredState;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/**
 * PostgreSQL 실행 저장소. 호출하는 쪽이 연 트랜잭션 하나 안에서 노드 상태 → 타이머 → 아웃박스 → 지표를 쓰고, 그 트랜잭션이 커밋된
 * 뒤에만 스트림 오프셋을 저장한다(BR-FLW-29).
 */
public class JdbcExecutionStore implements ExecutionStore {

    private final NodeStateRepository states;
    private final TimerRepository timers;
    private final OutboxRepository outbox;
    private final MetricBuffer metrics;
    private final Clock clock;

    public JdbcExecutionStore(NodeStateRepository states, TimerRepository timers, OutboxRepository outbox,
                              MetricBuffer metrics, Clock clock) {
        this.states = states;
        this.timers = timers;
        this.outbox = outbox;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public StoredState lockState(UUID flowId, long organizationId, String nodeId, String targetKey) {
        return states.lock(organizationId, flowId, nodeId, targetKey);
    }

    @Override
    public void writeState(UUID flowId, long organizationId, String nodeId, String targetKey, JsonNode state,
                           JsonNode stateConfig) {
        states.update(organizationId, flowId, nodeId, targetKey, state, stateConfig, clock.instant());
    }

    @Override
    public long countWaitingTimers(UUID flowId, long organizationId) {
        return timers.countWaiting(organizationId, flowId);
    }

    @Override
    public long insertTimer(UUID flowId, long organizationId, int flowVersion, String nodeId, String targetKey, TimerKind kind,
                            Instant dueAt, JsonNode context) {
        return timers.insert(organizationId, flowId, flowVersion, nodeId, targetKey, kind, dueAt, context, clock.instant());
    }

    @Override
    public void cancelTimer(long organizationId, long timerId) {
        timers.cancel(organizationId, timerId);
    }

    @Override
    public boolean insertOutbox(UUID flowId, long organizationId, int flowVersion, String nodeId, String triggerMessageId,
                                ActionDraft action) {
        return outbox.insert(organizationId, action.idempotencyKey(), action.kind(), action.exchange(), action.routingKey(),
                action.payload().toString(), flowId, flowVersion, nodeId, triggerMessageId, clock.instant());
    }

    @Override
    public void addMetrics(UUID flowId, long organizationId, String nodeId, Instant minute, NodeMetrics delta) {
        metrics.add(organizationId, flowId, nodeId, minute, delta);
    }
}
