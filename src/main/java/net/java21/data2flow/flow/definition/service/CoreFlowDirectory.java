package net.java21.data2flow.flow.definition.service;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.definition.domain.FlowRuntime;
import net.java21.data2flow.flow.definition.domain.RuntimeSnapshot;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * core-api의 플로우·공간 기준 정보(내부 API). 시험은 대역을 쓴다. M4에서 더한 조회(비상 정지·유지보수·공간 기기·스크립트·기기 태그·과거
 * 텔레메트리)는 core가 아직 경로를 열지 않았으면(404) 빈 값으로 다룬다.
 */
public interface CoreFlowDirectory {

    /** API-FLW-80. {@code sinceVersion}과 같으면(바뀐 것 없음) 빈 값 */
    Optional<RuntimeSnapshot> runtime(Long sinceVersion);

    /** API-FLW-81. 없거나 실행 대상이 아니면 빈 값 */
    Optional<FlowRuntime> flow(UUID flowId);

    /** API-DEV-128: 공간을 측정하는 기기 ID */
    Set<Long> measuringDevices(long organizationId, long spaceId, boolean includeDescendants);

    /** API-DEV-128(관계 무관): 공간(하위 포함)의 기기 ID. 비상 정지 범위 판정(BR-FLW-19) */
    default Set<Long> spaceDevices(long organizationId, long spaceId, boolean includeDescendants) {
        return Set.of();
    }

    /** 진행 중인 비상 정지(API-ACT-21 내부판 {@code GET /internal/core/emergency-stops?active=true}): {@code [{emergencyStopId, scope}]} */
    default List<JsonNode> activeEmergencyStops() {
        return List.of();
    }

    /** 진행 중인 유지보수 창(API-OPS-24): {@code [{targetType, targetId, descendantSpaceIds?, pauseAutomation, endsAt}]} */
    default List<JsonNode> activeMaintenance() {
        return List.of();
    }

    /** 스크립트 실행 묶음(API-SCR-32): {@code [{scriptId, organizationId, kind, versionId, versionNo, code, …}]} */
    default List<JsonNode> scriptBundle(long organizationId) {
        return List.of();
    }

    /** 기기 태그(API-DEV-122 {@code tags[]}). 모르면 빈 목록 */
    default List<String> deviceTags(long organizationId, long deviceId) {
        return List.of();
    }

    /**
     * 과거 텔레메트리 한 쪽(과거 재생 FLW-03.06, core가 pipeline 시계열을 읽어 줌). 측정 시각 순서.
     *
     * @param cursor 이전 쪽의 {@code nextCursor}. 처음은 null
     */
    default TelemetryPage telemetryHistory(long organizationId, List<Long> deviceIds, Instant from, Instant to, String cursor,
                                           int size) {
        return new TelemetryPage(List.of(), null, 0L);
    }

    /**
     * 과거 텔레메트리 한 쪽.
     *
     * @param items      표준 텔레메트리(측정 시각 순서)
     * @param nextCursor 다음 쪽. 없으면 null
     * @param total      전체 건수(알면). 모르면 null
     */
    record TelemetryPage(List<CanonicalTelemetry> items, String nextCursor, Long total) {
    }
}
