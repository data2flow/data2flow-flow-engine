package net.java21.data2flow.flow.definition.domain;

import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.flow.plan.domain.Overlay;

import java.util.Set;
import java.util.UUID;

/**
 * 엔진이 실행할 플로우 하나(core API-FLW-80·81 응답 항목): 실행 중인 버전의 정의와 상태·오버레이.
 *
 * @param flowId          플로우 ID
 * @param organizationId  조직
 * @param name            이름(로그·디버그)
 * @param kind            FLOW·RULE·CATCH
 * @param status          ACTIVE·DEGRADED(실행), PAUSED(트리거 버림·타이머 보류), DISABLED·DELETED(내림, 대기 타이머 취소)
 * @param activeVersion   실행할 버전
 * @param definition      그 버전의 정의
 * @param overlay         바이패스·디버그(버전 없이 즉시 반영)
 * @param rateLimitPerSec 초당 실행 한도(FLW-05.04, 버전에 포함). 0이면 기본 100
 * @param pauseMode       일시 정지 중 트리거 DROP·BUFFER(FLW-08.04)
 */
public record FlowRuntime(UUID flowId, long organizationId, String name, String kind, String status, int activeVersion,
                          FlowDefinition definition, Overlay overlay, int rateLimitPerSec, String pauseMode) {

    public FlowRuntime(UUID flowId, long organizationId, String name, String kind, String status, int activeVersion,
                       FlowDefinition definition, Overlay overlay) {
        this(flowId, organizationId, name, kind, status, activeVersion, definition, overlay, 0, "DROP");
    }

    private static final Set<String> RUNNING = Set.of("ACTIVE", "DEGRADED");
    private static final Set<String> LOADED = Set.of("ACTIVE", "DEGRADED", "PAUSED");

    /** 트리거를 받아 실행하는 상태 */
    public boolean running() {
        return RUNNING.contains(status);
    }

    /** 엔진에 실행 계획을 두는 상태(PAUSED는 두되 실행하지 않음) */
    public boolean loadable() {
        return LOADED.contains(status) && activeVersion > 0 && definition != null;
    }
}
