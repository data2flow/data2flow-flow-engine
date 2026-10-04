package net.java21.data2flow.flow.runtime.domain;

import net.java21.data2flow.flow.plan.domain.LoadedFlow;

/**
 * 커밋된 실행을 받는 곳(추적 저장 FLW-03.04, 라이브 뷰 카운터 FLW-03.01·03.03, 오류율 감시 FLW-08.03). 메시지 처리 트랜잭션이 커밋된
 * 뒤에만 부르므로 되돌린 실행은 오지 않는다. 손실을 견디는 관측용이라 예외를 던져도 처리는 계속된다.
 */
public interface ExecutionListener {

    /** 실행 하나가 커밋되었다 */
    void executed(LoadedFlow flow, ExecutionReport report);

    /** 트리거를 실행하지 않았다(일시 정지 DROP·single 모드·한도). 사유와 수 */
    default void dropped(LoadedFlow flow, String reason, int count) {
        // 기본 무시
    }
}
