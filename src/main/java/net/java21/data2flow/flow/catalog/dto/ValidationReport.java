package net.java21.data2flow.flow.catalog.dto;

import net.java21.data2flow.flow.plan.domain.FlowValidationError;

import java.util.List;

/**
 * API-FLW-84 응답. {@code errors}가 비어 있으면 엔진이 이 정의를 실행할 수 있다. core-api는 비어 있지 않으면 API-FLW-07을
 * 400 {@code FLOW_VALIDATION_FAILED}로 거절하고 {@code errors}를 그대로 응답 {@code errors[]}에 싣는다(api-rules {@code {field, code, message}}).
 */
public record ValidationReport(List<FlowValidationError> errors) {
}
