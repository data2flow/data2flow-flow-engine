package net.java21.data2flow.flow.runtime.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import net.java21.data2flow.flow.plan.domain.Jsons;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.JsonNode;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * {@code flow_paused_triggers}(ERD flow.md §3.4): 일시 정지 BUFFER 모드에서 보관한 트리거(reason PAUSED, FLW-08.04·BR-FLW-25, 1시간)와
 * parallel 모드에서 동시 실행 수가 찬 동안 기다리는 트리거(reason QUEUED, FLW-05.07). 꺼낼 때는 {@code FOR UPDATE SKIP LOCKED}로 잠근다.
 */
@Repository
public class BufferedTriggerRepository {

    private final JdbcClient jdbc;

    public BufferedTriggerRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 보관한 트리거 한 행. message = {triggerNodeId, body, triggerMessageId, targetKey, runKey} */
    public record Row(long id, long organizationId, UUID flowId, String targetKey, String reason, JsonNode message,
                      Instant receivedAt) {
    }

    public void insert(long organizationId, UUID flowId, String targetKey, String reason, JsonNode message, Instant receivedAt) {
        jdbc.sql("""
                        INSERT INTO flow_paused_triggers (organization_id, flow_id, target_key, reason, message, received_at)
                        VALUES (:org, :flow, :key, :reason, CAST(:message AS jsonb), :at)""")
                .param("org", organizationId).param("flow", flowId).param("key", targetKey).param("reason", reason)
                .param("message", message.toString()).param("at", Timestamp.from(receivedAt)).update();
    }

    /** 가장 오래된 것부터 잠근다(같은 대상 키는 받은 순서). runKey가 null이면 플로우 전체 */
    public List<Row> lockOldest(long organizationId, UUID flowId, String reason, String targetKey, int limit) {
        return jdbc.sql("""
                        SELECT id, organization_id, flow_id, target_key, reason, message::text, received_at FROM flow_paused_triggers
                         WHERE organization_id = :org AND flow_id = :flow AND reason = :reason
                           AND (CAST(:key AS varchar) IS NULL OR target_key = :key)
                         ORDER BY received_at, id LIMIT :limit FOR UPDATE SKIP LOCKED""")
                .param("org", organizationId).param("flow", flowId).param("reason", reason).param("key", targetKey)
                .param("limit", limit)
                .query((rs, n) -> new Row(rs.getLong(1), rs.getLong(2), rs.getObject(3, UUID.class), rs.getString(4),
                        rs.getString(5), Jsons.MAPPER.readTree(rs.getString(6)), rs.getTimestamp(7).toInstant())).list();
    }

    public void delete(long organizationId, long id) {
        jdbc.sql("DELETE FROM flow_paused_triggers WHERE id = :id AND organization_id = :org")
                .param("id", id).param("org", organizationId).update();
    }

    /** 플로우를 한 인스턴스만 비우도록 트랜잭션 잠금(pg_try_advisory_xact_lock). 다른 인스턴스가 비우는 중이면 false */
    @OrganizationScopeExempt("행을 읽지 않는 권고 잠금. 키는 플로우 ID(UUID)")
    public boolean tryLockFlow(UUID flowId) {
        return Boolean.TRUE.equals(jdbc.sql("SELECT pg_try_advisory_xact_lock(:key)")
                .param("key", flowId.getMostSignificantBits() ^ flowId.getLeastSignificantBits()).query(Boolean.class).single());
    }

    public long count(long organizationId, UUID flowId, String reason) {
        return jdbc.sql("SELECT count(*) FROM flow_paused_triggers WHERE organization_id = :org AND flow_id = :flow AND reason = :reason")
                .param("org", organizationId).param("flow", flowId).param("reason", reason).query(Long.class).single();
    }

    /** 이 배포 조직에서 보관 중인 트리거가 있는 플로우 */
    @OrganizationScopeExempt("배포 조직 목록(organizationIds)으로 좁힌다(ADR-030)")
    public List<UUID> flowsWithBuffered(java.util.Collection<Long> organizationIds, String reason) {
        if (organizationIds.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("SELECT DISTINCT flow_id FROM flow_paused_triggers WHERE organization_id = ANY(:orgs) AND reason = :reason")
                .param("orgs", organizationIds.toArray(Long[]::new)).param("reason", reason).query(UUID.class).list();
    }

    /** 보관 기한(1시간)이 지난 행을 지우고 플로우별 지운 수(BR-FLW-25: 버리고 건수를 기록) */
    @OrganizationScopeExempt("기한이 지난 행 정리는 조직과 무관한 시스템 보관 정책")
    public Map<UUID, Long> deleteExpired(Instant before) {
        Map<UUID, Long> out = new java.util.HashMap<>();
        jdbc.sql("DELETE FROM flow_paused_triggers WHERE received_at < :before RETURNING flow_id")
                .param("before", Timestamp.from(before))
                .query(rs -> {
                    out.merge(rs.getObject(1, UUID.class), 1L, Long::sum);
                });
        return out;
    }
}
