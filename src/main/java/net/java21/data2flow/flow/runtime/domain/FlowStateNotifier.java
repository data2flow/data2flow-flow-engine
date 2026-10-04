package net.java21.data2flow.flow.runtime.domain;

import net.java21.data2flow.contracts.message.event.FlowStateChanged;

/** EVT-FLW-03 {@code flow.state.changed} 발행(엔진 판정: DEGRADED·RUNAWAY·CYCLE·RECOVERED). 소비 core-api가 상태 저장·MAJOR 알람 */
@FunctionalInterface
public interface FlowStateNotifier {

    void notify(long organizationId, FlowStateChanged change);

    FlowStateNotifier NONE = (organizationId, change) -> {
    };
}
