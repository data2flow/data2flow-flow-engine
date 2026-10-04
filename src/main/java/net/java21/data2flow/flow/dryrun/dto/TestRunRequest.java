package net.java21.data2flow.flow.dryrun.dto;

import net.java21.data2flow.contracts.flow.FlowDefinition;
import tools.jackson.databind.JsonNode;

/**
 * API-FLW-12 내부 요청 {@code POST /internal/flow/test-runs}(FLW-api 부록 A).
 *
 * @param flowId         플로우 ID(UUID, 멱등 키·추적 표시용)
 * @param organizationId 조직(없으면 입력 텔레메트리의 조직)
 * @param version        버전(정의가 없을 때 적재된 계획과 맞춰 봄)
 * @param definition     시험할 정의(편집 중인 초안)
 * @param input          {@code {message: CanonicalTelemetry}} 또는 {@code {body: 플로우 메시지}}
 * @param startNodeId    시작 노드(없으면 트리거)
 */
public record TestRunRequest(String flowId, String organizationId, Integer version, FlowDefinition definition, JsonNode input,
                             String startNodeId) {
}
