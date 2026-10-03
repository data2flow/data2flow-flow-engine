package net.java21.data2flow.flow.catalog.controller;

import jakarta.validation.Valid;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.contracts.web.ListApiResponse;
import net.java21.data2flow.contracts.web.PageParams;
import net.java21.data2flow.flow.catalog.dto.ValidateRequest;
import net.java21.data2flow.flow.catalog.dto.ValidationReport;
import net.java21.data2flow.flow.definition.service.FlowSynchronizer;
import net.java21.data2flow.flow.plan.service.NodeTypeRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/**
 * 내부 API(ADR-021, 호출자 core-api). 정본: design/api/FLW-api.md §8.
 * <ul>
 *   <li>API-FLW-83 {@code GET /internal/flow/node-types}: 엔진 노드 레지스트리(core API-FLW-30이 그대로 내보냄, TC-FLW-031)</li>
 *   <li>API-FLW-84 {@code POST /internal/flow/definitions/validate}: 엔진 컴파일러로 정의를 검증(적용 전 검증 API-FLW-06·07의 노드 설정·
 *       포트·순환 검사). 계획은 적재하지 않는다</li>
 * </ul>
 */
@RestController
public class InternalCatalogController {

    private final NodeTypeRegistry registry;
    private final FlowSynchronizer synchronizer;

    public InternalCatalogController(NodeTypeRegistry registry, FlowSynchronizer synchronizer) {
        this.registry = registry;
        this.synchronizer = synchronizer;
    }

    @GetMapping("/internal/flow/node-types")
    public ListApiResponse<FlowNodeType> nodeTypes() {
        List<FlowNodeType> catalog = registry.catalog();
        return ListApiResponse.of(PageParams.of(1, 100), catalog, catalog.size());
    }

    @PostMapping("/internal/flow/definitions/validate")
    public ApiResponse<ValidationReport> validate(@Valid @RequestBody ValidateRequest request) {
        long organizationId;
        UUID flowId;
        try {
            organizationId = Long.parseLong(request.organizationId());
            flowId = request.flowId() == null ? new UUID(0, 0) : UUID.fromString(request.flowId());
        } catch (IllegalArgumentException e) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST);
        }
        return ApiResponse.success(new ValidationReport(synchronizer.validate(flowId, organizationId, 0, request.definition())));
    }
}
