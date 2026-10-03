package net.java21.data2flow.flow.definition.domain;

import java.util.List;

/**
 * core API-FLW-80 응답: 이 배포 조직의 실행 대상 플로우 전체.
 *
 * @param version        목록 버전(바뀌면 커짐). 다음 호출의 {@code sinceVersion}
 * @param organizationId 배포 조직(ADR-030). 엔진의 폴러·릴레이가 이 조직의 행만 다룬다
 * @param flows          ACTIVE·DEGRADED·PAUSED 플로우
 */
public record RuntimeSnapshot(long version, Long organizationId, List<FlowRuntime> flows) {
}
