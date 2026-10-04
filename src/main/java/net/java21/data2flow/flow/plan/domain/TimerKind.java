package net.java21.data2flow.flow.plan.domain;

import java.util.Set;

/** 지속 타이머 종류({@code flow_timers.kind}, ERD flow.md §3.2) */
public enum TimerKind {
    /** {@code flow.delay}: 지정 시간 뒤 메시지를 내보낸다 */
    DELAY,
    /** {@code flow.waitUntil} */
    WAIT_UNTIL,
    /** 조건 재확인: {@code condition.threshold}의 지속 시간({@code for}) 판정, {@code condition.noData} 무수신 판정 */
    RECHECK,
    /** 사람 승인 대기 */
    APPROVAL,
    /** 노드 재시도(FLW-08.01, BR-FLW-21): 실패한 노드를 백오프 뒤 다시 실행한다 */
    RETRY,
    /** 제어 결과 대기(action.control ok·failed 포트): EVT-ACT-01 끝 상태가 오면 이어서, 만기가 되면 failed(TIMEOUT) */
    AWAIT_RESULT;

    /** 실행이 "진행 중"임을 뜻하는 종류(FLW-05.07 실행 모드). RECHECK는 노드 상태의 판정 대기라 실행으로 세지 않는다 */
    public static final Set<TimerKind> RUN_KINDS = Set.of(DELAY, WAIT_UNTIL, APPROVAL, RETRY, AWAIT_RESULT);
}
