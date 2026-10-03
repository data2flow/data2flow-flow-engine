package net.java21.data2flow.flow.plan.domain;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;

import java.util.Optional;

/** 텔레메트리로 시작하는 트리거 노드(FLW-05.01). 관심 없는 메시지면 빈 값 */
public interface TriggerNode extends CompiledNode {

    Optional<FlowMessage> match(CanonicalTelemetry telemetry);

    @Override
    default void onMessage(FlowMessage message, NodeContext context) {
        context.emit(outputs().getFirst(), message);
    }
}
