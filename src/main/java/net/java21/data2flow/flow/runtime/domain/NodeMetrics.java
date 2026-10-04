package net.java21.data2flow.flow.runtime.domain;

import java.util.Arrays;

/**
 * 노드 하나의 지표 증가분(flow_metric_minutes 한 행에 더함, FLW-05.03 "오류 지표 +1", FLW-05.05의 바탕). 플로우 단위 행(노드 ID
 * {@value #FLOW_NODE})은 실행 수·오류 실행 수·실행 시간 합·지연 분포·행동 수·버린 트리거를 담는다.
 */
public final class NodeMetrics {

    /** 플로우 단위 지표 행의 노드 ID */
    public static final String FLOW_NODE = "*";
    /** 지연 분포 경계(ms, 위쪽 포함): 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 그 이상 */
    public static final long[] BUCKET_BOUNDS_MS = {1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000};

    private int processed;
    private int errors;
    private int dropped;
    private long totalMicros;
    private int actionsCommand;
    private int actionsNotify;
    private int actionsSink;
    private final int[] buckets = new int[BUCKET_BOUNDS_MS.length + 1];

    public void processed(long micros) {
        processed++;
        totalMicros += micros;
    }

    /** 실행 하나(플로우 행): 처리 시간을 분포에도 넣는다 */
    public void execution(long micros) {
        processed(micros);
        buckets[bucket(micros / 1000.0)]++;
    }

    public void error() {
        errors++;
    }

    public void drop() {
        dropped++;
    }

    public void command() {
        actionsCommand++;
    }

    public void notifyAction() {
        actionsNotify++;
    }

    public void sink() {
        actionsSink++;
    }

    /** 행동 종류로 센다 */
    public void action(String kind) {
        switch (kind) {
            case "COMMAND", "SCENE" -> actionsCommand++;
            case "NOTIFY" -> actionsNotify++;
            case "SINK" -> actionsSink++;
            default -> {
                // 알람 신호 등은 행동 지표에 넣지 않는다
            }
        }
    }

    static int bucket(double ms) {
        for (int i = 0; i < BUCKET_BOUNDS_MS.length; i++) {
            if (ms <= BUCKET_BOUNDS_MS[i]) {
                return i;
            }
        }
        return BUCKET_BOUNDS_MS.length;
    }

    /** 다른 증가분을 더한다 */
    public void addAll(NodeMetrics other) {
        processed += other.processed;
        errors += other.errors;
        dropped += other.dropped;
        totalMicros += other.totalMicros;
        actionsCommand += other.actionsCommand;
        actionsNotify += other.actionsNotify;
        actionsSink += other.actionsSink;
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] += other.buckets[i];
        }
    }

    public int processed() {
        return processed;
    }

    public int errors() {
        return errors;
    }

    public int dropped() {
        return dropped;
    }

    public long totalMs() {
        return totalMicros / 1000;
    }

    public long totalMicros() {
        return totalMicros;
    }

    public int actionsCommand() {
        return actionsCommand;
    }

    public int actionsNotify() {
        return actionsNotify;
    }

    public int actionsSink() {
        return actionsSink;
    }

    public int[] buckets() {
        return Arrays.copyOf(buckets, buckets.length);
    }
}
