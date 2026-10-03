package net.java21.data2flow.flow.plan.domain;

import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/**
 * 노드 실행 문맥. 실행기가 메시지 처리 트랜잭션 하나에 묶어 만든다: 상태·타이머·아웃박스가 같은 트랜잭션에 기록되고(BR-FLW-29),
 * 드라이런(시험 실행·재생)에서는 임시 저장소를 쓴다(BR-FLW-11).
 */
public interface NodeContext {

    UUID flowId();

    int flowVersion();

    long organizationId();

    String nodeId();

    /** 처리 시각(주입된 시계) */
    Instant now();

    /** 드라이런이면 true: 행동은 기록만 한다 */
    boolean dryRun();

    /** 이 노드의 대상 키 상태. 없으면 null. 같은 대상 키를 다른 실행이 동시에 바꾸지 못하게 잠근다 */
    JsonNode state(String targetKey);

    /** 상태를 저장한다(null이면 비움). 대상 키당 256KB를 넘으면 {@code NODE_STATE_TOO_LARGE}로 노드가 실패하고 이전 상태가 남는다 */
    void saveState(String targetKey, JsonNode state);

    /** 지속 타이머를 만든다({@code flow_timers}). 돌려준 ID로 취소한다 */
    long scheduleTimer(TimerKind kind, String targetKey, Instant dueAt, JsonNode context);

    /** 대기 중인 타이머를 취소한다(이미 발화했으면 아무 일도 없음) */
    void cancelTimer(long timerId);

    /** 행동 요청을 아웃박스에 쓴다(멱등 키가 같으면 한 번만) */
    void action(ActionDraft action);

    /** 출력 포트로 메시지를 내보낸다 */
    void emit(String port, FlowMessage message);

    /**
     * 노드 실패. error 포트에 와이어가 있으면 {@code {…원래 메시지, error:{nodeId, errorType, message, attempts}}}가 나가고, 없으면 그 갈래만
     * 끝나고 오류 지표가 1 오른다(FLW-05.03).
     */
    void fail(FlowMessage message, String errorType, String detail);

    /** 디버그 샘플(debug.log 노드) */
    void debug(DebugSample sample);
}
