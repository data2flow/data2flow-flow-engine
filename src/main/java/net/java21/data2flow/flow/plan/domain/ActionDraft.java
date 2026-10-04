package net.java21.data2flow.flow.plan.domain;

import tools.jackson.databind.JsonNode;

/**
 * 아웃박스에 쓸 행동 요청 하나(EVT-FLW-05, EVT-RUL-01). 노드가 만들고 실행기가 {@code flow_outboxes}에 {@code ON CONFLICT DO NOTHING}으로 넣는다.
 *
 * @param kind           COMMAND, NOTIFY, SINK, SCENE, WORK_ORDER, ANALYSIS, EVENT
 * @param exchange       보낼 exchange(기본 {@code data2flow.actions}, 알람 신호는 {@code data2flow.events})
 * @param routingKey     라우팅 키(예: {@code command}, {@code sink}, {@code alarm.signal})
 * @param idempotencyKey 멱등 키 {@code sha256(flowId, nodeId, triggerMessageId[, 분할 인덱스])}. 버전 미포함(BR-FLW-13)
 * @param payload        보낼 본문 전체(ActionRequest 또는 DomainEvent JSON)
 * @param summary        추적·드라이런 표시용 요약(예: {@code Thermostat.set(cool,24)})
 */
public record ActionDraft(String kind, String exchange, String routingKey, String idempotencyKey, JsonNode payload,
                          String summary) {

    /** 비상 정지·유지보수가 막는 기기 제어 행동인가(BR-FLW-19: 제어·장면) */
    public boolean controlsDevices() {
        return "COMMAND".equals(kind) || "SCENE".equals(kind);
    }
}
