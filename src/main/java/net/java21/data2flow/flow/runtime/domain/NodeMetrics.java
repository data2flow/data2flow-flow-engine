package net.java21.data2flow.flow.runtime.domain;

/**
 * 노드 하나의 지표 증가분(flow_metric_minutes 한 행에 더함, FLW-05.03 "오류 지표 +1", FLW-05.05의 바탕).
 */
public final class NodeMetrics {

    private int processed;
    private int errors;
    private long totalMicros;
    private int actionsCommand;

    public void processed(long micros) {
        processed++;
        totalMicros += micros;
    }

    public void error() {
        errors++;
    }

    public void command() {
        actionsCommand++;
    }

    /** 다른 증가분을 더한다 */
    public void addAll(NodeMetrics other) {
        processed += other.processed;
        errors += other.errors;
        totalMicros += other.totalMicros;
        actionsCommand += other.actionsCommand;
    }

    public int processed() {
        return processed;
    }

    public int errors() {
        return errors;
    }

    public long totalMs() {
        return totalMicros / 1000;
    }

    public int actionsCommand() {
        return actionsCommand;
    }
}
