package net.java21.data2flow.flow.runtime.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

/**
 * API-FLW-14 플로우 지표(내부 {@code GET /internal/flow/flows/{flow-id}/metrics}, core가 같은 모양으로 중계).
 *
 * @param summary 기간 합계
 * @param nodes   노드별
 * @param series  시간 구간별(step)
 */
public record FlowMetricsResponse(Summary summary, List<Node> nodes, List<Point> series) {

    /**
     * @param executions      실행 수
     * @param errors          오류로 끝난 실행 수
     * @param errorRate       오류율(0~1)
     * @param avgMs           평균 실행 시간
     * @param p95Ms           p95 실행 시간(분포 구간 상한, 1·2·5·10·20·50·100·200·500·1000·2000·5000ms)
     * @param actions         행동 수
     * @param droppedTriggers 버린 트리거(일시 정지·single 모드·안전장치·보관 기한)
     */
    public record Summary(long executions, long errors, double errorRate, double avgMs, double p95Ms, Actions actions,
                          long droppedTriggers) {
    }

    public record Actions(long command, @JsonProperty("notify") long notifyCount, long sink) {
    }

    public record Node(String nodeId, long processed, long errors, double avgMs) {
    }

    public record Point(Instant t, long executions, long errors) {
    }
}
