package net.java21.data2flow.flow.plan.domain;

import net.java21.data2flow.contracts.flow.FlowDefinition;

import java.util.Locale;

/**
 * 플로우 실행 모드(FLW-05.07, BR-FLW-18). 같은 실행 키(기본: 트리거 기기)에서 실행이 "진행 중"이면(대기·재시도·결과 대기 타이머가 남아 있음)
 * 새 트리거를 어떻게 다룰지 정한다.
 *
 * <ul>
 *   <li>{@code queued}(기본): 같은 기기의 메시지는 파티션 순서대로 처리한다(M3 동작, design §3.1). 대기 중인 실행이 있어도 새 실행을 시작한다.</li>
 *   <li>{@code single}: 진행 중인 실행이 있으면 새 트리거를 버리고 "single 모드로 건너뜀"을 기록한다(버린 트리거 지표).</li>
 *   <li>{@code restart}: 진행 중인 실행의 대기 타이머를 취소하고 새로 시작한다.</li>
 *   <li>{@code parallel}: 진행 중인 실행이 {@code max}(기본 10)개 미만이면 시작하고, 넘으면 보관했다가 하나가 끝나면 순서대로 시작한다.</li>
 * </ul>
 *
 * @param concurrency queued, single, restart, parallel
 * @param keyBy       실행 키: deviceId(기본), spaceId, none(플로우 전체 하나)
 * @param max         parallel 동시 실행 수(1~100)
 */
public record ExecutionMode(Concurrency concurrency, KeyBy keyBy, int max) {

    public static final ExecutionMode DEFAULT = new ExecutionMode(Concurrency.QUEUED, KeyBy.DEVICE, 10);
    public static final int MAX_PARALLEL = 100;

    public enum Concurrency { QUEUED, SINGLE, RESTART, PARALLEL }

    public enum KeyBy { DEVICE, SPACE, NONE }

    /** 정의의 {@code mode}를 읽는다. 모르는 값은 {@link NodeConfigException}(경로 {@code mode.*}) */
    public static ExecutionMode of(FlowDefinition.Mode mode) {
        if (mode == null) {
            return DEFAULT;
        }
        Concurrency c = switch (mode.concurrency() == null ? "queued" : mode.concurrency().toLowerCase(Locale.ROOT)) {
            case "queued" -> Concurrency.QUEUED;
            case "single" -> Concurrency.SINGLE;
            case "restart" -> Concurrency.RESTART;
            case "parallel" -> Concurrency.PARALLEL;
            default -> throw new NodeConfigException("mode.concurrency", "실행 모드는 queued, single, restart, parallel 중 하나입니다: "
                    + mode.concurrency());
        };
        KeyBy k = switch (mode.keyBy() == null ? "deviceid" : mode.keyBy().toLowerCase(Locale.ROOT)) {
            case "deviceid", "device" -> KeyBy.DEVICE;
            case "spaceid", "space" -> KeyBy.SPACE;
            case "none", "flow" -> KeyBy.NONE;
            default -> throw new NodeConfigException("mode.keyBy", "실행 키는 deviceId, spaceId, none 중 하나입니다: " + mode.keyBy());
        };
        int max = mode.max() == null ? 10 : mode.max();
        if (max < 1 || max > MAX_PARALLEL) {
            throw new NodeConfigException("mode.max", "동시 실행 수는 1~" + MAX_PARALLEL + "입니다: " + max);
        }
        return new ExecutionMode(c, k, max);
    }

    /** 트리거 메시지의 실행 키(예: {@code device:15}, {@code space:31}, {@code *}) */
    public String runKey(FlowMessage message) {
        return switch (keyBy) {
            case DEVICE -> {
                String d = Jsons.text(message.body(), "deviceId");
                yield d == null ? message.targetKey() : "device:" + d;
            }
            case SPACE -> {
                String s = Jsons.text(message.body(), "spaceId");
                yield s == null ? message.targetKey() : "space:" + s;
            }
            case NONE -> "*";
        };
    }
}
