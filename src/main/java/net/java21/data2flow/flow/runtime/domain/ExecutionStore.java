package net.java21.data2flow.flow.runtime.domain;

import net.java21.data2flow.flow.plan.domain.ActionDraft;
import net.java21.data2flow.flow.plan.domain.TimerKind;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/**
 * 실행 하나가 쓰는 저장소. 운영은 메시지 처리 트랜잭션 하나에 묶인 PostgreSQL({@code flow_node_state}·{@code flow_timers}·
 * {@code flow_outboxes}·{@code flow_metric_minutes}, BR-FLW-29), 시험 실행·단위 시험은 메모리({@link InMemoryExecutionStore}, BR-FLW-11).
 */
public interface ExecutionStore {

    /** 노드 상태를 잠그고 읽는다(행이 없으면 만들고 잠근다). 비어 있으면 null */
    JsonNode lockState(UUID flowId, long organizationId, String nodeId, String targetKey);

    /** 노드 상태를 쓴다. null이면 빈 상태 */
    void writeState(UUID flowId, long organizationId, String nodeId, String targetKey, JsonNode state);

    /** 지속 타이머를 만든다 */
    long insertTimer(UUID flowId, long organizationId, int flowVersion, String nodeId, String targetKey, TimerKind kind,
                     Instant dueAt, JsonNode context);

    /** 대기 중인 타이머를 취소한다 */
    void cancelTimer(long organizationId, long timerId);

    /** 아웃박스에 쓴다. 같은 멱등 키가 이미 있으면 false(BR-FLW-13) */
    boolean insertOutbox(UUID flowId, long organizationId, int flowVersion, String nodeId, String triggerMessageId,
                         ActionDraft action);

    /** 노드별 분 단위 지표를 더한다({@code flow_metric_minutes}) */
    void addMetrics(UUID flowId, long organizationId, String nodeId, Instant minute, NodeMetrics delta);
}
