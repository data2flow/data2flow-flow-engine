package net.java21.data2flow.flow.plan.domain;

import tools.jackson.databind.JsonNode;

/**
 * 라이브 뷰 샘플 하나(EVT-FLW-01 {@code node.sample}). 커밋 뒤 손실 허용으로 {@code data2flow.debug}에 낸다.
 *
 * @param nodeId    노드
 * @param messageId 원인 메시지 ID
 * @param direction in 또는 out
 * @param port      나간 포트(out일 때)
 * @param payload   메시지 본문(또는 debug.log가 고른 필드)
 * @param forced    debug.log 노드나 overlay.debug가 켠 샘플이면 true(상한 50건/초)
 */
public record DebugSample(String nodeId, String messageId, String direction, String port, JsonNode payload, boolean forced) {
}
