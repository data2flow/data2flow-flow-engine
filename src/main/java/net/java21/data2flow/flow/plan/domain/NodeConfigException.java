package net.java21.data2flow.flow.plan.domain;

/**
 * 노드 설정 오류(컴파일 실패). {@code path}는 노드 설정 안의 경로(예: {@code config.value}, {@code config.target.spaceId}).
 * 적용 검증 응답에서는 {@code nodes[<노드 ID>].config.value}처럼 노드를 붙여 {@code errors[].field}가 된다(api-rules §5).
 */
public class NodeConfigException extends RuntimeException {

    private final String path;
    private final String code;

    public NodeConfigException(String path, String code, String message) {
        super(message, null, false, false);
        this.path = path;
        this.code = code;
    }

    public NodeConfigException(String path, String message) {
        this(path, "INVALID_CONFIG", message);
    }

    public String path() {
        return path;
    }

    public String code() {
        return code;
    }
}
