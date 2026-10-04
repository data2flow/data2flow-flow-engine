package net.java21.data2flow.flow.rule.dto;

import net.java21.data2flow.contracts.flow.FlowDefinition;

/**
 * 규칙 컴파일 결과. core-api는 이 정의로 규칙의 내부 플로우(kind=RULE) 새 버전을 저장하고 적용한다(규칙 버전 = 플로우 버전, BR-RUL-01).
 *
 * @param definition 플로우 정의(노드 ID는 역할별 고정: 규칙을 고쳐도 지속 시간·연속 횟수 상태가 이어짐)
 * @param target     알람 대상 종류: DEVICE(기기별) 또는 SPACE(공간 집계)
 */
public record RuleCompileResponse(FlowDefinition definition, String target) {
}
