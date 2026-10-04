package net.java21.data2flow.flow.common;

import net.java21.data2flow.contracts.error.ErrorCode;

/**
 * flow-engine 내부 API 오류 코드(FLW-api §2·§8, RUL-api). 문구는 {@code messages*.properties}의 {@code error.<코드>}(4개 언어, ADR-037).
 * core-api는 엔진의 4xx를 그대로 전한다.
 */
public enum FlowEngineErrorCode implements ErrorCode {
    /** 시험 실행 입력이 없거나 읽을 수 없음(API-FLW-12) */
    FLOW_TEST_INPUT_INVALID(400),
    /** 정의 검증 실패(errors[{field, code, message}]) */
    FLOW_VALIDATION_FAILED(400),
    /** 과거 재생 기간이 7일을 넘거나 메시지가 너무 많음(API-FLW-13) */
    FLOW_REPLAY_TOO_LARGE(400),
    /** 규칙 조건을 플로우로 바꿀 수 없음(API-RUL-02 → 컴파일) */
    RULE_CONDITION_INVALID(400);

    private final int httpStatus;

    FlowEngineErrorCode(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    @Override
    public String code() {
        return name();
    }

    @Override
    public int httpStatus() {
        return httpStatus;
    }
}
