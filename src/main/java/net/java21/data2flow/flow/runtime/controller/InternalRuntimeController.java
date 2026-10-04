package net.java21.data2flow.flow.runtime.controller;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.flow.liveview.service.TraceQueryService;
import net.java21.data2flow.flow.runtime.dto.FlowMetricsResponse;
import net.java21.data2flow.flow.runtime.service.FlowMetricsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.JsonNode;

import java.util.UUID;

/**
 * 내부 API(ADR-021, 호출자 core-api, FLW-api §2·§8).
 * <ul>
 *   <li>API-FLW-14 {@code GET /internal/flow/flows/{flow-id}/metrics?window=1h|24h|7d&step=1m|1h}: 플로우 지표(FLW-05.05). 응답 모양은
 *       외부 API와 같고 core가 그대로 중계한다(ADR-047의 503 {@code FLOW_METRICS_UNAVAILABLE}은 엔진이 응답하지 않을 때만)</li>
 *   <li>API-FLW-41 {@code GET /internal/flow/traces/{message-id}?flowId=}: 실행 추적(FLW-03.04). 1시간 지났거나 보관하지 않은 메시지는 404</li>
 * </ul>
 */
@RestController
public class InternalRuntimeController {

    private final FlowMetricsService metrics;
    private final TraceQueryService traces;

    public InternalRuntimeController(FlowMetricsService metrics, TraceQueryService traces) {
        this.metrics = metrics;
        this.traces = traces;
    }

    @GetMapping("/internal/flow/flows/{flow-id}/metrics")
    public ApiResponse<FlowMetricsResponse> metrics(@PathVariable("flow-id") UUID flowId,
                                                    @RequestParam(value = "window", required = false) String window,
                                                    @RequestParam(value = "step", required = false) String step) {
        return ApiResponse.success(metrics.metrics(flowId, window, step));
    }

    @GetMapping("/internal/flow/traces/{message-id}")
    public ApiResponse<JsonNode> trace(@PathVariable("message-id") String messageId,
                                       @RequestParam(value = "flowId", required = false) UUID flowId) {
        return ApiResponse.success(traces.find(messageId, flowId)
                .orElseThrow(() -> new BusinessException(CommonErrorCode.RESOURCE_NOT_FOUND)));
    }
}
