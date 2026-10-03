package net.java21.data2flow.flow.plan.domain;

/** 지속 타이머 종류({@code flow_timers.kind}, ERD flow.md §3.2) */
public enum TimerKind {
    /** {@code flow.delay}: 지정 시간 뒤 메시지를 내보낸다 */
    DELAY,
    /** {@code flow.waitUntil}(M4) */
    WAIT_UNTIL,
    /** 조건 재확인: {@code condition.threshold}의 지속 시간({@code for}) 판정 */
    RECHECK,
    /** 사람 승인 대기(M4) */
    APPROVAL
}
