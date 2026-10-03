package net.java21.data2flow.flow.plan.domain;

/**
 * 플로우 검증·컴파일 오류 하나(API-FLW-06·07 {@code FLOW_VALIDATION_FAILED}의 {@code errors[]}, api-rules §5 {@code {field, code, message}}).
 *
 * @param field   위치: {@code definition}, {@code nodes}, {@code nodes[n-thr00001]}, {@code nodes[n-thr00001].config.value},
 *                {@code wires[2]}, {@code wires[2].port}
 * @param code    CYCLE, UNCONNECTED, TYPE_MISMATCH, INVALID_CONFIG, NO_TRIGGER, UNKNOWN_NODE_TYPE, UNKNOWN_PORT, INVALID_DEFINITION, LIMIT
 * @param message 사람이 읽는 설명
 */
public record FlowValidationError(String field, String code, String message) {

    public static String node(String nodeId) {
        return "nodes[" + nodeId + "]";
    }

    public static String wire(int index) {
        return "wires[" + index + "]";
    }
}
