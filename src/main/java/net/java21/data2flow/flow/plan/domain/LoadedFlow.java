package net.java21.data2flow.flow.plan.domain;

import java.util.UUID;

/**
 * 인스턴스에 적재된 플로우: 실행 계획(버전) + 상태 + 오버레이. {@code FlowRegistry}가 {@code AtomicReference}로 통째로 바꿔 끼운다.
 *
 * @param flowId         플로우
 * @param organizationId 조직
 * @param name           이름
 * @param status         ACTIVE·DEGRADED(실행), PAUSED(트리거 버림·타이머 보류)
 * @param plan           실행 계획
 * @param overlay        바이패스·디버그
 */
public record LoadedFlow(UUID flowId, long organizationId, String name, String status, ExecutionPlan plan, Overlay overlay) {

    public boolean running() {
        return "ACTIVE".equals(status) || "DEGRADED".equals(status);
    }

    public int version() {
        return plan.version();
    }
}
