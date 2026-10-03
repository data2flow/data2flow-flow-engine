package net.java21.data2flow.flow.apply.dto;

import java.time.Instant;
import java.util.List;

/**
 * API-FLW-82 응답: 플로우의 인스턴스별 적용 상태(편집기 "모든 인스턴스 적용됨", API-FLW-02 {@code applyStatus}의 원천).
 *
 * @param flowId    플로우
 * @param instances 최근 10분 안에 보고한 인스턴스
 */
public record ApplyStatusResponse(String flowId, List<Instance> instances) {

    /** @param instanceId 파드 이름, @param appliedVersion 적용 버전, @param overlayRevision 오버레이 리비전, @param reportedAt 보고 시각 */
    public record Instance(String instanceId, int appliedVersion, long overlayRevision, Instant reportedAt) {
    }
}
