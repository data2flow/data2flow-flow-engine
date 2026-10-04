package net.java21.data2flow.flow.runtime.domain;

import net.java21.data2flow.flow.plan.domain.ActionDraft;
import net.java21.data2flow.flow.plan.domain.DebugSample;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 실행 하나의 결과(추적 API-FLW-41·지표 FLW-05.05·라이브 뷰 FLW-03의 원천). 메시지 하나는 계획 하나로 처리되므로(BR-FLW-06) 보고서마다
 * 처리한 버전이 하나다.
 *
 * @param flowId           플로우
 * @param version          처리한 플로우 버전
 * @param triggerMessageId 원인 메시지 ID
 * @param startedAt        처리 시각(재생은 재생 중인 시각)
 * @param durationMicros   처리 시간(µs)
 * @param steps            거친 노드 순서와 나간 포트
 * @param actions          아웃박스에 쓴 행동(드라이런이면 기록만)
 * @param samples          커밋 뒤에 낼 디버그 샘플
 * @param errors           노드 오류 수(최종 실패만, 재시도 예약은 세지 않음)
 * @param metrics          노드별 지표
 */
public record ExecutionReport(UUID flowId, int version, String triggerMessageId, Instant startedAt, long durationMicros,
                              List<Step> steps, List<ActionDraft> actions, List<DebugSample> samples, int errors,
                              Map<String, NodeMetrics> metrics) {

    /** 오류 포트·한도로 끝난 노드가 있는가 */
    public boolean failed() {
        return errors > 0;
    }

    /** 디버그를 켠 노드를 지났는가(추적 보관 대상, FLW-03.04) */
    public boolean debugged() {
        return samples.stream().anyMatch(DebugSample::forced);
    }

    /**
     * 노드 하나의 처리(API-FLW-41 Trace의 steps와 같은 정보).
     *
     * @param nodeId         노드
     * @param type           노드 종류
     * @param inMicros       실행 시작부터 이 노드에 들어온 시각(µs)
     * @param durationMicros 노드 처리 시간(µs)
     * @param input          입력 본문(타이머 발화면 null)
     * @param ports          나간 포트(오류면 error)
     * @param errorType      오류 종류. 없으면 null
     * @param errorMessage   오류 설명
     * @param outputs        나간 메시지(포트, 본문)
     * @param actions        행동 기록(행동 노드)
     */
    public record Step(String nodeId, String type, long inMicros, long durationMicros, JsonNode input, List<String> ports,
                       String errorType, String errorMessage, List<Output> outputs, List<ActionRecord> actions) {

        public Step(String nodeId, List<String> ports, String errorType, String errorMessage) {
            this(nodeId, null, 0, 0, null, ports, errorType, errorMessage, List.of(), List.of());
        }
    }

    /** 나간 메시지 하나 */
    public record Output(String port, JsonNode payload) {
    }

    /**
     * 행동 기록(추적의 action).
     *
     * @param kind           COMMAND, NOTIFY, SINK, SCENE, EVENT
     * @param idempotencyKey 멱등 키(건너뛰었으면 null일 수 있음)
     * @param dryRun         드라이런
     * @param skipped        건너뛴 사유(bypassed, EMERGENCY_STOP, MAINTENANCE). 나갔으면 null
     * @param summary        요약(예: {@code Thermostat.set{mode=cool, targetTemperature=24}})
     */
    public record ActionRecord(String kind, String idempotencyKey, boolean dryRun, String skipped, String summary) {
    }
}
