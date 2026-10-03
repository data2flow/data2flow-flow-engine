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

    private final Map<String, JsonNode> states = new LinkedHashMap<>();
    private final Map<Long, Timer> timers = new LinkedHashMap<>();
    private final Map<String, Outbox> outbox = new LinkedHashMap<>();
    private final Map<String, NodeMetrics> metrics = new LinkedHashMap<>();
    private final List<Long> cancelled = new ArrayList<>();
    private final AtomicLong sequence = new AtomicLong();

    private static String key(UUID flowId, String nodeId, String targetKey) {
        return flowId + "|" + nodeId + "|" + targetKey;
    }

    @Override
    public JsonNode lockState(UUID flowId, long organizationId, String nodeId, String targetKey) {
        JsonNode s = states.get(key(flowId, nodeId, targetKey));
        return s == null ? null : s.deepCopy();
    }

    @Override
    public void writeState(UUID flowId, long organizationId, String nodeId, String targetKey, JsonNode state) {
        if (state == null || state.isEmpty()) {
            states.remove(key(flowId, nodeId, targetKey));
        } else {
            states.put(key(flowId, nodeId, targetKey), state.deepCopy());
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
    public boolean insertOutbox(UUID flowId, long organizationId, int flowVersion, String nodeId, String triggerMessageId,
                                ActionDraft action) {
        return outbox.putIfAbsent(action.idempotencyKey(), new Outbox(flowId, flowVersion, nodeId, triggerMessageId, action)) == null;
    }

    @Override
    public void addMetrics(UUID flowId, long organizationId, String nodeId, Instant minute, NodeMetrics delta) {
        NodeMetrics m = metrics.computeIfAbsent(flowId + "|" + nodeId, k -> new NodeMetrics());
        for (int i = 0; i < delta.errors(); i++) {
            m.error();
        }
        for (int i = 0; i < delta.processed(); i++) {
            m.processed(0);
        }
        for (int i = 0; i < delta.actionsCommand(); i++) {
            m.command();
        }
    }

    /** 만기가 된 타이머를 꺼낸다(만기 순) */
    public List<Timer> takeDue(Instant now) {
        List<Timer> due = timers.values().stream().filter(t -> !t.dueAt().isAfter(now))
                .sorted(java.util.Comparator.comparing(Timer::dueAt).thenComparingLong(Timer::id)).toList();
        due.forEach(t -> timers.remove(t.id()));
        return due;
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
        return states.get(key(flowId, nodeId, targetKey));
    }

    public NodeMetrics metrics(UUID flowId, String nodeId) {
        return metrics.getOrDefault(flowId + "|" + nodeId, new NodeMetrics());
    }
}
