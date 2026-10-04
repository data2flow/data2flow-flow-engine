package net.java21.data2flow.flow.dryrun.controller;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.flow.dryrun.dto.ReplayRequest;
import net.java21.data2flow.flow.dryrun.dto.TestRunRequest;
import net.java21.data2flow.flow.dryrun.dto.TestRunResponse;
import net.java21.data2flow.flow.dryrun.service.ReplayService;
import net.java21.data2flow.flow.dryrun.service.TestRunService;
import net.java21.data2flow.flow.plan.domain.Jsons;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.JsonNode;

import java.util.UUID;

/**
 * 드라이런 내부 API(ADR-021, 호출자 core-api, FLW-api §2). 행동은 기록만 하고 운영 노드 상태를 바꾸지 않는다(BR-FLW-11).
 * <ul>
 *   <li>API-FLW-12 {@code POST /internal/flow/test-runs} → {@code {trace}}(FLW-03.05)</li>
 *   <li>API-FLW-13 {@code POST /internal/flow/replays} → 202 {@code {jobId, status}}, {@code GET /internal/flow/replays/{job-id}},
 *       {@code POST /internal/flow/replays/{job-id}/cancel}(FLW-03.06). core-api는 외부 {@code /flow-replays/{job-id}}를 이 조회로 중계한다</li>
 * </ul>
 */
@RestController
public class InternalDryRunController {

    private final TestRunService testRuns;
    private final ReplayService replays;

    public InternalDryRunController(TestRunService testRuns, ReplayService replays) {
        this.testRuns = testRuns;
        this.replays = replays;
    }

    @PostMapping("/internal/flow/test-runs")
    public ApiResponse<TestRunResponse> testRun(@RequestBody TestRunRequest request) {
        return ApiResponse.success(new TestRunResponse(testRuns.run(request)));
    }

    @PostMapping("/internal/flow/replays")
    public ResponseEntity<ApiResponse<JsonNode>> replay(@RequestBody ReplayRequest request) {
        UUID jobId = replays.submit(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(ApiResponse.success(
                Jsons.object().put("jobId", jobId.toString()).put("status", "QUEUED")));
    }

    @GetMapping("/internal/flow/replays/{job-id}")
    public ApiResponse<JsonNode> replayStatus(@PathVariable("job-id") UUID jobId) {
        return ApiResponse.success(replays.find(jobId).map(ReplayService::view)
                .orElseThrow(() -> new BusinessException(CommonErrorCode.RESOURCE_NOT_FOUND)));
    }

    @PostMapping("/internal/flow/replays/{job-id}/cancel")
    public ApiResponse<JsonNode> cancel(@PathVariable("job-id") UUID jobId) {
        replays.cancel(jobId);
        return ApiResponse.success(replays.find(jobId).map(ReplayService::view)
                .orElseThrow(() -> new BusinessException(CommonErrorCode.RESOURCE_NOT_FOUND)));
    }
}
