package net.java21.data2flow.flow.runtime.domain;

import net.java21.data2flow.flow.plan.domain.ActionDraft;
import net.java21.data2flow.flow.plan.domain.DebugSample;

import java.util.List;
import java.util.Map;

/**
 * 실행 하나의 결과(추적·지표·디버그용).
 *
 * @param steps   거친 노드 순서와 나간 포트
 * @param actions 아웃박스에 쓴 행동(드라이런이면 기록만)
 * @param samples 커밋 뒤에 낼 디버그 샘플
 * @param errors  노드 오류 수
 * @param metrics 노드별 지표
 */
public record ExecutionReport(List<Step> steps, List<ActionDraft> actions, List<DebugSample> samples, int errors,
                              Map<String, NodeMetrics> metrics) {

    /**
     * 노드 하나의 처리(API-FLW-41 Trace의 steps와 같은 정보).
     *
     * @param nodeId       노드
     * @param ports        나간 포트(오류면 error)
     * @param errorType    오류 종류. 없으면 null
     * @param errorMessage 오류 설명
     * @param outputs      나간 메시지(포트, 본문)
     */
    public record Step(String nodeId, List<String> ports, String errorType, String errorMessage, List<Output> outputs) {

        public Step(String nodeId, List<String> ports, String errorType, String errorMessage) {
            this(nodeId, ports, errorType, errorMessage, List.of());
        }
    }

    /** 나간 메시지 하나 */
    public record Output(String port, tools.jackson.databind.JsonNode payload) {
    }
}
