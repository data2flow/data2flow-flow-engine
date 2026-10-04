package net.java21.data2flow.flow.runtime.domain;

import net.java21.data2flow.flow.plan.domain.ActionDraft;
import net.java21.data2flow.flow.plan.domain.TimerKind;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 메모리 실행 저장소: 시험 실행·과거 재생(BR-FLW-11, 운영 상태를 바꾸지 않음)과 단위 시험({@code FlowTestHarness})이 쓴다. 스레드 안전하지
 * 않다(실행 하나 또는 시험 하나가 쓴다).
 */
public class InMemoryExecutionStore implements ExecutionStore {

    /** 대기 중인 타이머 */
    public record Timer(long id, UUID flowId, int flowVersion, String nodeId, String targetKey, TimerKind kind, Instant dueAt,
                        JsonNode context) {
    }

    /** 아웃박스 행 */
    public record Outbox(UUID flowId, int flowVersion, String nodeId, String triggerMessageId, ActionDraft action) {
    }

    private final Map<String, StoredState> states = new LinkedHashMap<>();
    private final Map<Long, Timer> timers = new LinkedHashMap<>();
    private final Map<String, Outbox> outbox = new LinkedHashMap<>();
    private final Map<String, NodeMetrics> metrics = new LinkedHashMap<>();
    private final List<Long> cancelled = new ArrayList<>();
    private final AtomicLong sequence = new AtomicLong();

    private static String key(UUID flowId, String nodeId, String targetKey) {
        return flowId + "|" + nodeId + "|" + targetKey;
    }

    @Override
    public StoredState lockState(UUID flowId, long organizationId, String nodeId, String targetKey) {
        StoredState s = states.get(key(flowId, nodeId, targetKey));
        return s == null ? StoredState.EMPTY
                : new StoredState(s.state() == null ? null : s.state().deepCopy(), s.stateConfig());
    }

    @Override
    public void writeState(UUID flowId, long organizationId, String nodeId, String targetKey, JsonNode state,
                           JsonNode stateConfig) {
        if (state == null || state.isEmpty()) {
            states.remove(key(flowId, nodeId, targetKey));
        } else {
            states.put(key(flowId, nodeId, targetKey), new StoredState(state.deepCopy(), stateConfig));
        }
    }

    @Override
    public long insertTimer(UUID flowId, long organizationId, int flowVersion, String nodeId, String targetKey, TimerKind kind,
                            Instant dueAt, JsonNode context) {
        long id = sequence.incrementAndGet();
        timers.put(id, new Timer(id, flowId, flowVersion, nodeId, targetKey, kind, dueAt, context.deepCopy()));
        return id;
    }

    @Override
    public void cancelTimer(long organizationId, long timerId) {
        if (timers.remove(timerId) != null) {
            cancelled.add(timerId);
        }
    }

    @Override
    public long countWaitingTimers(UUID flowId, long organizationId) {
        return timers.values().stream().filter(t -> t.flowId().equals(flowId)).count();
    }

    @Override
    public boolean insertOutbox(UUID flowId, long organizationId, int flowVersion, String nodeId, String triggerMessageId,
                                ActionDraft action) {
        return outbox.putIfAbsent(action.idempotencyKey(), new Outbox(flowId, flowVersion, nodeId, triggerMessageId, action)) == null;
    }

    @Override
    public void addMetrics(UUID flowId, long organizationId, String nodeId, Instant minute, NodeMetrics delta) {
        metrics.computeIfAbsent(flowId + "|" + nodeId, k -> new NodeMetrics()).addAll(delta);
    }

    /** 만기가 된 타이머를 꺼낸다(만기 순) */
    public List<Timer> takeDue(Instant now) {
        List<Timer> due = timers.values().stream().filter(t -> !t.dueAt().isAfter(now))
                .sorted(java.util.Comparator.comparing(Timer::dueAt).thenComparingLong(Timer::id)).toList();
        due.forEach(t -> timers.remove(t.id()));
        return due;
    }

    /** 가장 이른 대기 타이머의 만기. 없으면 null */
    public Instant nextDue() {
        return timers.values().stream().map(Timer::dueAt).min(Instant::compareTo).orElse(null);
    }

    /** 대기 타이머를 하나 꺼낸다(결과 대기 이어 붙이기 시험) */
    public Timer take(long timerId) {
        return timers.remove(timerId);
    }

    public Map<Long, Timer> timers() {
        return Map.copyOf(timers);
    }

    public List<Long> cancelled() {
        return List.copyOf(cancelled);
    }

    public List<Outbox> outbox() {
        return List.copyOf(outbox.values());
    }

    public JsonNode state(UUID flowId, String nodeId, String targetKey) {
        StoredState s = states.get(key(flowId, nodeId, targetKey));
        return s == null ? null : s.state();
    }

    /** 상태 행(상태 지문 포함) */
    public StoredState stored(UUID flowId, String nodeId, String targetKey) {
        return states.get(key(flowId, nodeId, targetKey));
    }

    /** 노드의 모든 대상 키 상태를 지운다(시험) */
    public void putState(UUID flowId, String nodeId, String targetKey, JsonNode state, JsonNode stateConfig) {
        states.put(key(flowId, nodeId, targetKey), new StoredState(state, stateConfig));
    }

    public NodeMetrics metrics(UUID flowId, String nodeId) {
        return metrics.getOrDefault(flowId + "|" + nodeId, new NodeMetrics());
    }
}
