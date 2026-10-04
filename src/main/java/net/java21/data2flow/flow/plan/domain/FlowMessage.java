package net.java21.data2flow.flow.plan.domain;

import tools.jackson.databind.node.ObjectNode;

/**
 * 노드 사이를 흐르는 메시지(FLW-api §5.1 "플로우 메시지"). 본문은 JSON 객체이고 텔레메트리 트리거가 만든 모양은
 * {@code {messageId, topic:"telemetry", organizationId, deviceId, spaceId, modelId, measuredAt, receivedAt, virtual, payload:{측정 키: 값}, metrics:[…]}}다.
 *
 * @param body             메시지 본문. 노드는 받은 본문을 고치지 않고 새 메시지를 내보낸다(엔진이 갈래마다 복사해 넘긴다)
 * @param triggerMessageId 이 실행을 일으킨 원인 메시지 ID(텔레메트리 messageId, 타이머 발화는 대기를 시작한 메시지). 행동 멱등 키에 쓴다(BR-FLW-13)
 * @param targetKey        실행 대상 키(노드 상태·타이머의 키, 예: {@code device:15}, {@code space:31})
 * @param runKey           실행 키(FLW-05.07 실행 모드, 예: {@code device:15}). 트리거가 정하고 타이머 문맥으로 이어진다. 없으면 null
 */
public record FlowMessage(ObjectNode body, String triggerMessageId, String targetKey, String runKey) {

    public FlowMessage {
        if (body == null || triggerMessageId == null || triggerMessageId.isBlank() || targetKey == null || targetKey.isBlank()) {
            throw new IllegalArgumentException("메시지에는 본문·원인 메시지 ID·대상 키가 필요합니다");
        }
    }

    public FlowMessage(ObjectNode body, String triggerMessageId, String targetKey) {
        this(body, triggerMessageId, targetKey, null);
    }

    /** 본문을 깊게 복사한 메시지(갈래마다 따로 고칠 수 있게) */
    public FlowMessage copy() {
        return new FlowMessage(body.deepCopy(), triggerMessageId, targetKey, runKey);
    }

    public FlowMessage withBody(ObjectNode newBody) {
        return new FlowMessage(newBody, triggerMessageId, targetKey, runKey);
    }

    public FlowMessage withTargetKey(String newKey) {
        return new FlowMessage(body, triggerMessageId, newKey, runKey);
    }

    public FlowMessage withRunKey(String newRunKey) {
        return new FlowMessage(body, triggerMessageId, targetKey, newRunKey);
    }
}
