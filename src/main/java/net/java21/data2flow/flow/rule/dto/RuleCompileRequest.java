package net.java21.data2flow.flow.rule.dto;

import tools.jackson.databind.JsonNode;

/**
 * 규칙 컴파일 요청(내부 {@code POST /internal/flow/rules/compile}, core-api가 규칙 저장 때 부름).
 *
 * @param organizationId 조직
 * @param ruleId         규칙 ID(알람 키 {@code rule:{ruleId}:{대상}})
 * @param rule           API-RUL-02 요청 모양 {@code {scope, condition, timeCondition?, severity, titleTemplate, autoClear?}}
 */
public record RuleCompileRequest(String organizationId, String ruleId, JsonNode rule) {
}
