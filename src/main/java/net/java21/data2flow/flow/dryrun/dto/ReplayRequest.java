package net.java21.data2flow.flow.dryrun.dto;

import net.java21.data2flow.contracts.flow.FlowDefinition;

import java.time.Instant;
import java.util.List;

/**
 * API-FLW-13 내부 요청 {@code POST /internal/flow/replays}(core-api가 버전의 정의를 실어 위임).
 *
 * @param flowId         플로우 ID
 * @param organizationId 조직
 * @param version        재생할 버전(표시용)
 * @param definition     그 버전의 정의
 * @param from           시작(포함)
 * @param to             끝(제외, from부터 7일 이하)
 * @param deviceIds      기기(없으면 조직 전체)
 */
public record ReplayRequest(String flowId, String organizationId, Integer version, FlowDefinition definition, Instant from,
                            Instant to, List<String> deviceIds) {
}
