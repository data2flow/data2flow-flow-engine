package net.java21.data2flow.flow.plan.domain;

import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;

/**
 * 노드 종류 SPI(design/flow-engine-and-live-reload.md §2.2). 새 노드 종류는 이 인터페이스 구현 하나와 카탈로그 항목
 * ({@code classpath:node-types/{type}.json}) 하나로 추가한다. 카탈로그는 API-FLW-83으로 core-api에 나가 API-FLW-30이 된다.
 */
public interface NodeType {

    /** 카탈로그 항목(설정 스키마·포트·상태 정책·권한·기본값) */
    FlowNodeType descriptor();

    default String type() {
        return descriptor().type();
    }

    /**
     * 노드 정의를 실행 가능한 노드로 바꾼다(설정 검증·사전 계산). 설정이 틀리면 {@link NodeConfigException}.
     * 결과는 불변이어야 한다(실행 계획을 여러 스레드가 함께 쓴다).
     */
    CompiledNode compile(FlowNode node, CompileContext context);
}
