package net.java21.data2flow.flow.plan.domain;

import tools.jackson.databind.JsonNode;

import java.time.Duration;

/**
 * 노드 재시도 정책(FLW-08.01, BR-FLW-21). 노드 정의의 {@code retry}가 있으면 그것을, 없으면 노드 종류의 기본값({@code defaults.retry})을 쓴다:
 * 일반 노드 0회, 행동 노드 3회(지수 백오프 1초·2배·최대 30초). 재시도는 지속 타이머(RETRY)로 미뤄서 하므로 같은 대상 키의 다음 메시지를
 * 막지 않는다. 마지막 실패는 error 포트로 {@code attempts = 재시도 + 1}과 함께 나간다.
 *
 * @param maxAttempts 재시도 횟수(0~10). 첫 실행은 세지 않는다
 * @param initial     첫 대기
 * @param multiplier  배수
 * @param max         최대 대기
 */
public record RetryPolicy(int maxAttempts, Duration initial, double multiplier, Duration max) {

    public static final RetryPolicy NONE = new RetryPolicy(0, Duration.ofSeconds(1), 2, Duration.ofSeconds(30));
    public static final int MAX_ATTEMPTS = 10;

    /** 정의({@code node.retry}) → 종류 기본값({@code defaults.retry}) 순서로 읽는다 */
    public static RetryPolicy of(JsonNode nodeRetry, JsonNode typeDefaults) {
        JsonNode base = typeDefaults == null ? null : typeDefaults.get("retry");
        int attempts = intValue(nodeRetry, "maxAttempts", intValue(base, "maxAttempts", 0));
        if (attempts < 0 || attempts > MAX_ATTEMPTS) {
            throw new NodeConfigException("retry.maxAttempts", "재시도 횟수는 0~" + MAX_ATTEMPTS + "입니다: " + attempts);
        }
        JsonNode nb = nodeRetry == null ? null : nodeRetry.get("backoff");
        JsonNode bb = base == null ? null : base.get("backoff");
        long initial = intValue(nb, "initialMs", intValue(bb, "initialMs", 1000));
        double multiplier = doubleValue(nb, "multiplier", doubleValue(bb, "multiplier", 2));
        long max = intValue(nb, "maxMs", intValue(bb, "maxMs", 30_000));
        if (initial < 100 || max < initial || multiplier < 1 || multiplier > 10) {
            throw new NodeConfigException("retry.backoff", "재시도 간격은 initialMs ≥ 100, maxMs ≥ initialMs, multiplier 1~10입니다");
        }
        return new RetryPolicy(attempts, Duration.ofMillis(initial), multiplier, Duration.ofMillis(max));
    }

    /** {@code attempt}번째(1부터) 실패 뒤 다음 시도까지의 대기 */
    public Duration delayAfter(int attempt) {
        double ms = initial.toMillis() * Math.pow(multiplier, Math.max(0, attempt - 1));
        return Duration.ofMillis((long) Math.min(ms, max.toMillis()));
    }

    private static int intValue(JsonNode node, String field, int fallback) {
        JsonNode v = node == null ? null : node.get(field);
        return v == null || !v.isNumber() ? fallback : v.asInt();
    }

    private static double doubleValue(JsonNode node, String field, double fallback) {
        JsonNode v = node == null ? null : node.get(field);
        return v == null || !v.isNumber() ? fallback : v.asDouble();
    }
}
