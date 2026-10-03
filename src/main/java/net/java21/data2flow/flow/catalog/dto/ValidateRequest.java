package net.java21.data2flow.flow.catalog.dto;

import jakarta.validation.constraints.NotNull;
import net.java21.data2flow.contracts.flow.FlowDefinition;

/**
 * API-FLW-84 요청.
 *
 * @param organizationId 조직(문자열 ID)
 * @param flowId         플로우(없으면 검증용 임시 ID)
 * @param definition     검증할 정의
 */
public record ValidateRequest(@NotNull String organizationId, String flowId, @NotNull FlowDefinition definition) {
}
