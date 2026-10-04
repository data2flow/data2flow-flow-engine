package net.java21.data2flow.flow.outbox.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * {@code flow_outboxes}(ERD flow.md §3.3, 아웃박스 표준 모양). 멱등 키 unique로 같은 행동은 한 번만 기록되고({@code ON CONFLICT DO NOTHING},
 * BR-FLW-13), 릴레이가 {@code FOR UPDATE SKIP LOCKED}로 가져가 publisher confirm 뒤 {@code sent_at}을 채운다.
 */
@Repository
public class OutboxRepository {

    private final JdbcClient jdbc;

    public OutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 보낼 행 */
    public record OutboxRow(long id, long organizationId, String idempotencyKey, String kind, String exchange, String routingKey,
                            String payload, int attempts) {
    }

    /** @return 새로 기록했으면 true, 같은 멱등 키가 이미 있으면 false */
    public boolean insert(long organizationId, String idempotencyKey, String kind, String exchange, String routingKey,
                          String payload, UUID flowId, int flowVersion, String nodeId, String triggerMessageId, Instant now) {
        return jdbc.sql("""
                        INSERT INTO flow_outboxes (organization_id, idempotency_key, kind, exchange, routing_key, payload, flow_id,
                                                   flow_version, node_id, trigger_message_id, created_at)
                        VALUES (:org, :key, :kind, :exchange, :routing, CAST(:payload AS jsonb), :flow, :version, :node, :trigger, :now)
                        ON CONFLICT (idempotency_key) DO NOTHING""")
                .param("org", organizationId).param("key", idempotencyKey).param("kind", kind).param("exchange", exchange)
                .param("routing", routingKey).param("payload", payload).param("flow", flowId).param("version", flowVersion)
                .param("node", nodeId).param("trigger", triggerMessageId.length() > 64 ? triggerMessageId.substring(0, 64) : triggerMessageId)
                .param("now", Timestamp.from(now)).update() == 1;
    }

    /** 보내지 않은 행을 잠근다(이 배포의 조직만, ADR-030) */
    @OrganizationScopeExempt("배포 조직 목록(organizationIds)으로 좁힌다(ADR-030)")
    public List<OutboxRow> lockUnsent(Collection<Long> organizationIds, int limit, Instant now) {
        if (organizationIds.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("""
                        SELECT id, organization_id, idempotency_key, kind, exchange, routing_key, payload::text, attempts
                          FROM flow_outboxes WHERE sent_at IS NULL AND organization_id = ANY(:orgs)
                           AND (next_attempt_at IS NULL OR next_attempt_at <= :now)
                         ORDER BY created_at, id LIMIT :limit FOR UPDATE SKIP LOCKED""")
                .param("orgs", organizationIds.toArray(Long[]::new)).param("limit", limit).param("now", Timestamp.from(now))
                .query((rs, n) -> new OutboxRow(rs.getLong(1), rs.getLong(2), rs.getString(3).trim(), rs.getString(4),
                        rs.getString(5), rs.getString(6), rs.getString(7), rs.getInt(8))).list();
    }

    /** 확인받은 행들을 한 번에 보냄으로 표시한다(릴레이 묶음) */
    @OrganizationScopeExempt("릴레이가 방금 잠근 행 ID로만 갱신한다(배포 조직 행만 잠금, ADR-030)")
    public void markSent(java.util.Collection<Long> ids, Instant now) {
        if (ids.isEmpty()) {
            return;
        }
        jdbc.sql("""
                        UPDATE flow_outboxes SET sent_at = :now, attempts = LEAST(attempts + 1, 32767), last_error = NULL,
                               next_attempt_at = NULL WHERE id = ANY(:ids)""")
                .param("now", Timestamp.from(now)).param("ids", ids.toArray(Long[]::new)).update();
    }

    public void markSent(long organizationId, long id, Instant now) {
        jdbc.sql("UPDATE flow_outboxes SET sent_at = :now, attempts = attempts + 1, last_error = NULL WHERE id = :id AND organization_id = :org")
                .param("now", Timestamp.from(now)).param("id", id).param("org", organizationId).update();
    }

    /** 실패를 기록하고 다음 시도 시각을 미룬다(재발행 간격) */
    public void markFailed(long organizationId, long id, String error, Instant nextAttemptAt) {
        jdbc.sql("""
                        UPDATE flow_outboxes SET attempts = LEAST(attempts + 1, 32767), last_error = :error, next_attempt_at = :next
                         WHERE id = :id AND organization_id = :org""")
                .param("error", error == null ? null : error.length() > 500 ? error.substring(0, 500) : error)
                .param("next", nextAttemptAt == null ? null : Timestamp.from(nextAttemptAt))
                .param("id", id).param("org", organizationId).update();
    }

    /** 보낸 지 7일 지난 행 정리(멱등 판정은 action의 executed_actions가 영구 보관, BR-ACT-02) */
    @OrganizationScopeExempt("보낸 행 정리는 조직과 무관한 시스템 보관 정책")
    public int deleteSent(Instant before) {
        return jdbc.sql("DELETE FROM flow_outboxes WHERE sent_at IS NOT NULL AND sent_at < :before")
                .param("before", Timestamp.from(before)).update();
    }

    /** 멱등 키로 행 수(시험·조회) */
    public long countByKey(long organizationId, String idempotencyKey) {
        return jdbc.sql("SELECT count(*) FROM flow_outboxes WHERE idempotency_key = :key AND organization_id = :org")
                .param("key", idempotencyKey).param("org", organizationId).query(Long.class).single();
    }
}
