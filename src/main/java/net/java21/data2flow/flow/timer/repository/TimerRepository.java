package net.java21.data2flow.flow.timer.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.TimerKind;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.JsonNode;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * {@code flow_timers}(ERD flow.md §3.2, 작업 큐 표준 모양). 만기가 된 행은 {@code SELECT … FOR UPDATE SKIP LOCKED}로 가져가므로
 * 인스턴스가 여럿이어도 한 행은 한 번만 발화한다(FLW-05.02, TC-FLW-097·098).
 */
@Repository
public class TimerRepository {

    private final JdbcClient jdbc;

    public TimerRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 대기 타이머 한 행 */
    public record TimerRow(long id, long organizationId, UUID flowId, int flowVersion, String nodeId, String targetKey,
                           TimerKind kind, Instant dueAt, JsonNode context, int attempts) {
    }

    /** 발화 후보(잠그지 않음) */
    public record Candidate(long id, long organizationId, UUID flowId, String nodeId, String targetKey) {
    }

    public long insert(long organizationId, UUID flowId, int flowVersion, String nodeId, String targetKey, TimerKind kind,
                       Instant dueAt, JsonNode context, Instant now) {
        return jdbc.sql("""
                        INSERT INTO flow_timers (organization_id, flow_id, flow_version, node_id, target_key, kind, due_at, context, created_at)
                        VALUES (:org, :flow, :version, :node, :key, :kind, :due, CAST(:context AS jsonb), :now) RETURNING id""")
                .param("org", organizationId).param("flow", flowId).param("version", flowVersion).param("node", nodeId)
                .param("key", targetKey).param("kind", kind.name()).param("due", Timestamp.from(dueAt))
                .param("context", context == null ? "{}" : context.toString()).param("now", Timestamp.from(now))
                .query(Long.class).single();
    }

    /** 대기 중이면 취소한다(이미 발화·취소됐으면 아무 일도 없음) */
    public int cancel(long organizationId, long timerId) {
        return jdbc.sql("UPDATE flow_timers SET status = 'CANCELLED' WHERE id = :id AND organization_id = :org AND status = 'WAITING'")
                .param("id", timerId).param("org", organizationId).update();
    }

    /**
     * 만기가 된 대기 타이머 후보(잠그지 않음). 이 배포의 조직(ADR-030: staging·prod가 같은 DB)과 이 인스턴스가 적재한 플로우만 본다.
     */
    @OrganizationScopeExempt("배포 조직 목록(organizationIds)으로 좁힌다(ADR-030)")
    public List<Candidate> findDue(Collection<Long> organizationIds, Collection<UUID> flowIds, Instant now, int limit) {
        if (organizationIds.isEmpty() || flowIds.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("""
                        SELECT id, organization_id, flow_id, node_id, target_key FROM flow_timers
                         WHERE status = 'WAITING' AND due_at <= :now AND organization_id = ANY(:orgs) AND flow_id = ANY(:flows)
                         ORDER BY due_at, id LIMIT :limit""")
                .param("now", Timestamp.from(now)).param("orgs", organizationIds.toArray(Long[]::new))
                .param("flows", flowIds.toArray(UUID[]::new)).param("limit", limit)
                .query((rs, n) -> new Candidate(rs.getLong(1), rs.getLong(2), rs.getObject(3, UUID.class), rs.getString(4),
                        rs.getString(5))).list();
    }

    /** 대기 중인 타이머를 잠근다. 다른 인스턴스가 잠갔거나 이미 발화·취소됐으면 빈 값({@code FOR UPDATE SKIP LOCKED}) */
    public Optional<TimerRow> lockWaiting(long organizationId, long timerId) {
        return jdbc.sql("""
                        SELECT id, organization_id, flow_id, flow_version, node_id, target_key, kind, due_at, context::text, attempts
                          FROM flow_timers WHERE id = :id AND organization_id = :org AND status = 'WAITING' FOR UPDATE SKIP LOCKED""")
                .param("id", timerId).param("org", organizationId)
                .query((rs, n) -> new TimerRow(rs.getLong(1), rs.getLong(2), rs.getObject(3, UUID.class), rs.getInt(4),
                        rs.getString(5), rs.getString(6), TimerKind.valueOf(rs.getString(7)), rs.getTimestamp(8).toInstant(),
                        Jsons.MAPPER.readTree(rs.getString(9)), rs.getInt(10))).optional();
    }

    public void markFired(long organizationId, long timerId, Instant now) {
        jdbc.sql("UPDATE flow_timers SET status = 'FIRED', fired_at = :now WHERE id = :id AND organization_id = :org")
                .param("now", Timestamp.from(now)).param("id", timerId).param("org", organizationId).update();
    }

    public void markCancelled(long organizationId, long timerId) {
        jdbc.sql("UPDATE flow_timers SET status = 'CANCELLED' WHERE id = :id AND organization_id = :org")
                .param("id", timerId).param("org", organizationId).update();
    }

    /** 발화 처리 실패 횟수를 올린다(별도 트랜잭션). 한도를 넘으면 CANCELLED */
    public void recordFailure(long organizationId, long timerId, int maxAttempts) {
        jdbc.sql("""
                        UPDATE flow_timers SET attempts = attempts + 1,
                               status = CASE WHEN attempts + 1 >= :max THEN 'CANCELLED' ELSE status END
                         WHERE id = :id AND organization_id = :org AND status = 'WAITING'""")
                .param("max", maxAttempts).param("id", timerId).param("org", organizationId).update();
    }

    /** 플로우를 끄거나 지우면 대기 타이머를 모두 취소한다(BR-FLW-18) */
    public int cancelFlow(long organizationId, UUID flowId) {
        return jdbc.sql("UPDATE flow_timers SET status = 'CANCELLED' WHERE flow_id = :flow AND organization_id = :org AND status = 'WAITING'")
                .param("flow", flowId).param("org", organizationId).update();
    }

    /** 플로우의 대기 타이머 수(BR-FLW-16 한도·시험) */
    public long countWaiting(long organizationId, UUID flowId) {
        return jdbc.sql("SELECT count(*) FROM flow_timers WHERE flow_id = :flow AND organization_id = :org AND status = 'WAITING'")
                .param("flow", flowId).param("org", organizationId).query(Long.class).single();
    }

    /**
     * 실행 키의 진행 중인 실행 수(FLW-05.07): 대기·재시도·결과 대기 타이머가 남은 서로 다른 실행(원인 메시지) 수.
     */
    public int countRunsInProgress(long organizationId, UUID flowId, String runKey) {
        return jdbc.sql("""
                        SELECT count(DISTINCT context->'@run'->>'id') FROM flow_timers
                         WHERE flow_id = :flow AND organization_id = :org AND status = 'WAITING'
                           AND (context->'@run'->>'key') = :key AND kind IN ('DELAY','WAIT_UNTIL','APPROVAL','RETRY','AWAIT_RESULT')""")
                .param("flow", flowId).param("org", organizationId).param("key", runKey).query(Integer.class).single();
    }

    /** restart 모드(BR-FLW-18): 실행 키의 진행 중인 실행 타이머를 모두 취소한다. 취소한 수 */
    public int cancelRuns(long organizationId, UUID flowId, String runKey) {
        return jdbc.sql("""
                        UPDATE flow_timers SET status = 'CANCELLED'
                         WHERE flow_id = :flow AND organization_id = :org AND status = 'WAITING'
                           AND (context->'@run'->>'key') = :key AND kind IN ('DELAY','WAIT_UNTIL','APPROVAL','RETRY','AWAIT_RESULT')""")
                .param("flow", flowId).param("org", organizationId).param("key", runKey).update();
    }

    /** 제어 결과를 기다리는 타이머(EVT-ACT-01의 멱등 키로 찾음). 잠그지 않음 */
    public List<Candidate> findAwaiting(long organizationId, String awaitKey) {
        return jdbc.sql("""
                        SELECT id, organization_id, flow_id, node_id, target_key FROM flow_timers
                         WHERE organization_id = :org AND status = 'WAITING' AND kind = 'AWAIT_RESULT' AND (context->>'awaitKey') = :key""")
                .param("org", organizationId).param("key", awaitKey)
                .query((rs, n) -> new Candidate(rs.getLong(1), rs.getLong(2), rs.getObject(3, UUID.class), rs.getString(4),
                        rs.getString(5))).list();
    }

    /** RESET된 노드의 대기 타이머 중 이전 버전이 만든 것을 취소한다(FLW-06.03 정리). 취소한 수 */
    public int cancelNodeBefore(long organizationId, UUID flowId, String nodeId, int version) {
        return jdbc.sql("""
                        UPDATE flow_timers SET status = 'CANCELLED'
                         WHERE flow_id = :flow AND organization_id = :org AND node_id = :node AND status = 'WAITING'
                           AND flow_version < :version AND kind IN ('RECHECK','WAIT_UNTIL')""")
                .param("flow", flowId).param("org", organizationId).param("node", nodeId).param("version", version).update();
    }

    /** 한 행(시험·조회) */
    public Optional<TimerRow> find(long organizationId, long timerId) {
        return jdbc.sql("""
                        SELECT id, organization_id, flow_id, flow_version, node_id, target_key, kind, due_at, context::text, attempts
                          FROM flow_timers WHERE id = :id AND organization_id = :org""")
                .param("id", timerId).param("org", organizationId)
                .query((rs, n) -> new TimerRow(rs.getLong(1), rs.getLong(2), rs.getObject(3, UUID.class), rs.getInt(4),
                        rs.getString(5), rs.getString(6), TimerKind.valueOf(rs.getString(7)), rs.getTimestamp(8).toInstant(),
                        Jsons.MAPPER.readTree(rs.getString(9)), rs.getInt(10))).optional();
    }

    /** 끝난 타이머 정리(7일, ERD README §14) */
    @OrganizationScopeExempt("끝난 행 정리는 조직과 무관한 시스템 보관 정책")
    public int deleteFinished(Instant before) {
        return jdbc.sql("DELETE FROM flow_timers WHERE status <> 'WAITING' AND created_at < :before")
                .param("before", Timestamp.from(before)).update();
    }
}
