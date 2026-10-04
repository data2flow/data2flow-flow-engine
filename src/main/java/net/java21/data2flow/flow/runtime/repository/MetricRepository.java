package net.java21.data2flow.flow.runtime.repository;

import net.java21.data2flow.flow.runtime.domain.NodeMetrics;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** {@code flow_metric_minutes}(ERD flow.md §3.5): 노드별 분 단위 지표. 인스턴스가 여럿이어도 더해진다 */
@Repository
public class MetricRepository {

    private final JdbcClient jdbc;

    public MetricRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void add(long organizationId, UUID flowId, String nodeId, Instant minute, NodeMetrics m) {
        int[] b = m.buckets();
        boolean anyBucket = java.util.Arrays.stream(b).anyMatch(x -> x > 0);
        Integer[] buckets = anyBucket ? java.util.Arrays.stream(b).boxed().toArray(Integer[]::new) : new Integer[0];
        jdbc.sql("""
                        INSERT INTO flow_metric_minutes (flow_id, node_id, minute, organization_id, processed, errors, dropped, total_ms,
                                                         actions_command, actions_notify, actions_sink, latency_buckets)
                        VALUES (:flow, :node, :minute, :org, :processed, :errors, :dropped, :ms, :commands, :notify, :sink, :buckets)
                        ON CONFLICT (flow_id, node_id, minute) DO UPDATE SET
                            processed = flow_metric_minutes.processed + EXCLUDED.processed,
                            errors = flow_metric_minutes.errors + EXCLUDED.errors,
                            dropped = flow_metric_minutes.dropped + EXCLUDED.dropped,
                            total_ms = flow_metric_minutes.total_ms + EXCLUDED.total_ms,
                            actions_command = flow_metric_minutes.actions_command + EXCLUDED.actions_command,
                            actions_notify = flow_metric_minutes.actions_notify + EXCLUDED.actions_notify,
                            actions_sink = flow_metric_minutes.actions_sink + EXCLUDED.actions_sink,
                            latency_buckets = COALESCE((SELECT array_agg(COALESCE(x, 0) + COALESCE(y, 0) ORDER BY i)
                                                          FROM unnest(flow_metric_minutes.latency_buckets, EXCLUDED.latency_buckets)
                                                               WITH ORDINALITY AS t(x, y, i)), '{}')""")
                .param("flow", flowId).param("node", nodeId).param("minute", Timestamp.from(minute)).param("org", organizationId)
                .param("processed", m.processed()).param("errors", m.errors()).param("dropped", m.dropped())
                .param("ms", m.totalMs()).param("commands", m.actionsCommand()).param("notify", m.actionsNotify())
                .param("sink", m.actionsSink()).param("buckets", buckets).update();
    }

    /** 분 단위 행 하나(지표 조회) */
    public record MinuteRow(String nodeId, Instant minute, long processed, long errors, long dropped, long totalMs,
                            long command, long notifyCount, long sink, long[] buckets) {
    }

    /** 기간 안의 행(API-FLW-14) */
    public java.util.List<MinuteRow> rows(long organizationId, UUID flowId, Instant from, Instant to) {
        return jdbc.sql("""
                        SELECT node_id, minute, processed, errors, dropped, total_ms, actions_command, actions_notify, actions_sink,
                               latency_buckets FROM flow_metric_minutes
                         WHERE flow_id = :flow AND organization_id = :org AND minute >= :from AND minute < :to
                         ORDER BY minute, node_id""")
                .param("flow", flowId).param("org", organizationId).param("from", Timestamp.from(from)).param("to", Timestamp.from(to))
                .query((rs, n) -> {
                    java.sql.Array a = rs.getArray(10);
                    Integer[] values = a == null ? new Integer[0] : (Integer[]) a.getArray();
                    long[] buckets = new long[values.length];
                    for (int i = 0; i < values.length; i++) {
                        buckets[i] = values[i] == null ? 0 : values[i];
                    }
                    return new MinuteRow(rs.getString(1), rs.getTimestamp(2).toInstant(), rs.getLong(3), rs.getLong(4), rs.getLong(5),
                            rs.getLong(6), rs.getLong(7), rs.getLong(8), rs.getLong(9), buckets);
                }).list();
    }

    /** 이 플로우의 지표 행이 있는 조직(내부 조회는 조직을 모른다). 없으면 빈 값 */
    @net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt("내부 API는 플로우 ID로 조직을 찾는다(플로우 ID는 UUID라 조직 사이에 겹치지 않음)")
    public java.util.Optional<Long> findOrganization(UUID flowId) {
        return jdbc.sql("SELECT organization_id FROM flow_metric_minutes WHERE flow_id = :flow LIMIT 1")
                .param("flow", flowId).query(Long.class).optional();
    }

    /** 플로우의 노드별 합계(시험·내부 조회) */
    public Map<String, long[]> totals(long organizationId, UUID flowId) {
        Map<String, long[]> out = new java.util.LinkedHashMap<>();
        jdbc.sql("""
                        SELECT node_id, sum(processed), sum(errors), sum(actions_command) FROM flow_metric_minutes
                         WHERE flow_id = :flow AND organization_id = :org GROUP BY node_id ORDER BY node_id""")
                .param("flow", flowId).param("org", organizationId)
                .query(rs -> {
                    out.put(rs.getString(1), new long[]{rs.getLong(2), rs.getLong(3), rs.getLong(4)});
                });
        return out;
    }
}
