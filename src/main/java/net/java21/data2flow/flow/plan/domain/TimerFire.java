package net.java21.data2flow.flow.plan.domain;

import tools.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * 만기가 된 지속 타이머 하나(노드의 {@link CompiledNode#onTimer}에 넘긴다).
 *
 * @param timerId     {@code flow_timers.id}
 * @param kind        종류
 * @param targetKey   대상 키
 * @param dueAt       만기 시각
 * @param context     타이머를 만들 때 넣은 문맥(재투입할 메시지 등)
 * @param createdVersion 타이머를 만든 플로우 버전
 */
public record TimerFire(long timerId, TimerKind kind, String targetKey, Instant dueAt, JsonNode context, int createdVersion) {
}
