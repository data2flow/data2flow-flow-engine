package net.java21.data2flow.flow.rule.controller;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.flow.common.FlowEngineErrorCode;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.rule.dto.RuleCompileRequest;
import net.java21.data2flow.flow.rule.dto.RuleCompileResponse;
import net.java21.data2flow.flow.rule.service.RuleFlowCompiler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/**
 * 규칙 → 플로우 컴파일 내부 API(RUL-01.01, BR-RUL-01, ADR-005). {@code POST /internal/flow/rules/compile} → {@code {definition, target}}.
 * 만든 정의는 엔진 컴파일러로 한 번 더 검사하고(노드 설정·포트·순환), 규칙 조건을 바꿀 수 없으면 400 {@code RULE_CONDITION_INVALID}
 * ({@code errors[{field, code, message}]}).
 */
@RestController
public class InternalRuleController {

    private final RuleFlowCompiler rules;
    private final FlowCompiler compiler;

    public InternalRuleController(RuleFlowCompiler rules, FlowCompiler compiler) {
        this.rules = rules;
        this.compiler = compiler;
    }

    @PostMapping("/internal/flow/rules/compile")
    public ApiResponse<RuleCompileResponse> compile(@RequestBody RuleCompileRequest request) {
        long organizationId;
        long ruleId;
        try {
            organizationId = Long.parseLong(request.organizationId());
            ruleId = Long.parseLong(request.ruleId());
        } catch (NumberFormatException | NullPointerException e) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST);
        }
        RuleFlowCompiler.Result result = rules.compile(ruleId, request.rule());
        FlowCompiler.Result check = compiler.compile(new UUID(0, ruleId), organizationId, 0, result.definition());
        if (!check.ok()) {
            List<FieldErrorDetail> errors = check.errors().stream()
                    .map(e -> new FieldErrorDetail(e.field(), e.code(), e.message())).toList();
            throw new BusinessException(FlowEngineErrorCode.RULE_CONDITION_INVALID, errors);
        }
        return ApiResponse.success(new RuleCompileResponse(result.definition(), result.target()));
    }
}
