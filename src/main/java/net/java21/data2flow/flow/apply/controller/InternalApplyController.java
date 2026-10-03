package net.java21.data2flow.flow.apply.controller;

import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.flow.apply.dto.ApplyStatusResponse;
import net.java21.data2flow.flow.apply.service.ApplyStatusService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * 내부 API(ADR-021: ClusterIP·토큰 없음, 호출자 core-api). API-FLW-82 {@code GET /internal/flow/flows/{flow-id}/apply-status}.
 * 정본: design/api/FLW-api.md §8.
 */
@RestController
public class InternalApplyController {

    private final ApplyStatusService service;

    public InternalApplyController(ApplyStatusService service) {
        this.service = service;
    }

    @GetMapping("/internal/flow/flows/{flow-id}/apply-status")
    public ApiResponse<ApplyStatusResponse> applyStatus(@PathVariable("flow-id") UUID flowId) {
        return ApiResponse.success(service.status(flowId));
    }
}
