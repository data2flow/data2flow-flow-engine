package net.java21.data2flow.flow.runtime.domain;

import net.java21.data2flow.flow.plan.domain.ActionDraft;
import net.java21.data2flow.flow.plan.domain.FlowMessage;

import java.util.Optional;

/**
 * 행동을 내보내기 전 확인(BR-FLW-19 비상 정지, OPS-05.02 유지보수 자동 제어 정지). 막으면 건너뛴 사유(예: {@code EMERGENCY_STOP},
 * {@code MAINTENANCE})를 돌려주고, 노드는 행동을 내보내지 않으며 추적에 "skipped(사유)"가 남는다.
 */
@FunctionalInterface
public interface ActionGuard {

    Optional<String> skipReason(long organizationId, ActionDraft action, FlowMessage trigger);

    ActionGuard NONE = (organizationId, action, trigger) -> Optional.empty();
}
