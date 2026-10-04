package net.java21.data2flow.flow.plan.domain;

import java.util.UUID;

/**
 * 인스턴스에 적재된 플로우: 실행 계획(버전) + 상태 + 오버레이 + 실행 설정. {@code FlowRegistry}가 {@code AtomicReference}로 통째로 바꿔 끼운다.
 *
 * @param flowId          플로우
 * @param organizationId  조직
 * @param name            이름
 * @param status          ACTIVE·DEGRADED(실행), PAUSED(트리거 버림·보관, 타이머 보류)
 * @param plan            실행 계획
 * @param overlay         바이패스·디버그
 * @param kind            FLOW·RULE·CATCH
 * @param rateLimitPerSec 초당 실행 한도(FLW-05.04, 기본 100, 최대 1,000)
 * @param pauseMode       일시 정지 중 트리거: DROP(기본) 또는 BUFFER(FLW-08.04)
 */
public record LoadedFlow(UUID flowId, long organizationId, String name, String status, ExecutionPlan plan, Overlay overlay,
                         String kind, int rateLimitPerSec, String pauseMode) {

    public static final int DEFAULT_RATE_LIMIT = 100;
    public static final int MAX_RATE_LIMIT = 1000;

    public LoadedFlow(UUID flowId, long organizationId, String name, String status, ExecutionPlan plan, Overlay overlay) {
        this(flowId, organizationId, name, status, plan, overlay, "FLOW", DEFAULT_RATE_LIMIT, "DROP");
    }

    public LoadedFlow {
        rateLimitPerSec = rateLimitPerSec <= 0 ? DEFAULT_RATE_LIMIT : Math.min(rateLimitPerSec, MAX_RATE_LIMIT);
        pauseMode = "BUFFER".equalsIgnoreCase(pauseMode) ? "BUFFER" : "DROP";
        kind = kind == null ? "FLOW" : kind;
    }

    public boolean running() {
        return "ACTIVE".equals(status) || "DEGRADED".equals(status);
    }

    public boolean paused() {
        return "PAUSED".equals(status);
    }

    public int version() {
        return plan.version();
    }

    public LoadedFlow withStatus(String newStatus) {
        return new LoadedFlow(flowId, organizationId, name, newStatus, plan, overlay, kind, rateLimitPerSec, pauseMode);
    }

    public LoadedFlow withPlan(ExecutionPlan newPlan) {
        return new LoadedFlow(flowId, organizationId, name, status, newPlan, overlay, kind, rateLimitPerSec, pauseMode);
    }
}
