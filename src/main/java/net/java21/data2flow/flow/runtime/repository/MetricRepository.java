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
        jdbc.sql("""
                        INSERT INTO flow_metric_minutes (flow_id, node_id, minute, organization_id, processed, errors, total_ms, actions_command)
                        VALUES (:flow, :node, :minute, :org, :processed, :errors, :ms, :commands)
                        ON CONFLICT (flow_id, node_id, minute) DO UPDATE SET
                            processed = flow_metric_minutes.processed + EXCLUDED.processed,
                            errors = flow_metric_minutes.errors + EXCLUDED.errors,
                            total_ms = flow_metric_minutes.total_ms + EXCLUDED.total_ms,
                            actions_command = flow_metric_minutes.actions_command + EXCLUDED.actions_command""")
                .param("flow", flowId).param("node", nodeId).param("minute", Timestamp.from(minute)).param("org", organizationId)
                .param("processed", m.processed()).param("errors", m.errors()).param("ms", m.totalMs())
                .param("commands", m.actionsCommand()).update();
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
