package net.java21.data2flow.flow.plan.domain;

import java.util.List;

/**
 * 컴파일된 노드(불변, 스레드 안전). 상태는 필드에 두지 않고 {@link NodeContext#state}로 읽고 쓴다(BR-FLW-29: PostgreSQL에 저장).
 */
public interface CompiledNode {

    /** 입력 메시지 하나를 처리한다. 출력은 {@link NodeContext#emit}, 실패는 {@link NodeContext#fail} */
    void onMessage(FlowMessage message, NodeContext context);

    /** 이 노드가 만든 지속 타이머가 만기가 되었다 */
    default void onTimer(TimerFire fire, NodeContext context) {
        // 타이머를 쓰지 않는 노드
    }

    /** 바이패스(overlay)일 때 행동 노드면 실행하지 않고 "bypassed"를 기록한다(BR-FLW-10). 변환·조건 노드는 첫 출력 포트로 넘긴다 */
    default boolean isAction() {
        return false;
    }

    /** 이 노드의 출력 포트(공통 error 제외). 첫 번째가 기본 포트(와이어에 port가 없을 때) */
    List<String> outputs();

    /** 실행 계획이 드레인 뒤 닫힐 때(live-reload §4 ⑥). 붙잡은 자원(JS 컨텍스트 등)을 놓는다 */
    default void close() {
        // 자원 없음
    }
}
