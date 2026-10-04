package net.java21.data2flow.flow.dryrun.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import net.java21.data2flow.flow.plan.domain.Jsons;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.JsonNode;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/** {@code flow_replay_jobs}: 과거 재생 작업(FLW-03.06). 인스턴스 하나가 잡아 실행하고 1분 넘게 소식이 없으면 다른 인스턴스가 넘겨받는다 */
@Repository
public class ReplayJobRepository {

    private final JdbcClient jdbc;

    public ReplayJobRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 작업 한 행 */
    public record Job(UUID id, long organizationId, UUID flowId, String status, JsonNode request, long processed, Long total,
                      JsonNode result, String error) {
    }

    public void insert(UUID id, long organizationId, UUID flowId, JsonNode request, Instant now) {
        jdbc.sql("""
                        INSERT INTO flow_replay_jobs (id, organization_id, flow_id, status, request, created_at)
                        VALUES (:id, :org, :flow, 'QUEUED', CAST(:request AS jsonb), :now)""")
                .param("id", id).param("org", organizationId).param("flow", flowId).param("request", request.toString())
                .param("now", Timestamp.from(now)).update();
    }

    /** 실행할 작업 하나를 잡는다(대기 중이거나, 실행 중인데 1분 넘게 소식이 없는 것) */
    @OrganizationScopeExempt("배포 조직 목록으로 좁힌다(ADR-030)")
    public Optional<Job> claim(java.util.Collection<Long> organizationIds, String instanceId, Instant now, Instant staleBefore) {
        if (organizationIds.isEmpty()) {
            return Optional.empty();
        }
        return jdbc.sql("""
                        UPDATE flow_replay_jobs SET status = 'RUNNING', locked_by = :instance, heartbeat_at = :now
                         WHERE id = (SELECT id FROM flow_replay_jobs
                                      WHERE organization_id = ANY(:orgs)
                                        AND (status = 'QUEUED' OR (status = 'RUNNING' AND heartbeat_at < :stale))
                                      ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED)
                        RETURNING id, organization_id, flow_id, status, request::text, processed, total, result::text, error""")
                .param("instance", instanceId).param("now", Timestamp.from(now)).param("stale", Timestamp.from(staleBefore))
                .param("orgs", organizationIds.toArray(Long[]::new))
                .query((rs, n) -> job(rs)).optional();
    }

    /** 진행을 기록한다. 취소되었으면 false */
    public boolean progress(long organizationId, UUID id, long processed, Long total, Instant now) {
        return jdbc.sql("""
                        UPDATE flow_replay_jobs SET processed = :processed, total = :total, heartbeat_at = :now
                         WHERE id = :id AND organization_id = :org AND status = 'RUNNING'""")
                .param("processed", processed).param("total", total).param("now", Timestamp.from(now)).param("id", id)
                .param("org", organizationId).update() == 1;
    }

    public void finish(long organizationId, UUID id, String status, JsonNode result, String error, long processed, Instant now) {
        jdbc.sql("""
                        UPDATE flow_replay_jobs SET status = :status, result = CAST(:result AS jsonb), error = :error,
                               processed = :processed, finished_at = :now, heartbeat_at = :now
                         WHERE id = :id AND organization_id = :org AND status = 'RUNNING'""")
                .param("status", status).param("result", result == null ? null : result.toString())
                .param("error", error == null ? null : error.length() > 500 ? error.substring(0, 500) : error)
                .param("processed", processed).param("now", Timestamp.from(now)).param("id", id).param("org", organizationId).update();
    }

    /** 대기·실행 중이면 취소한다. 바뀌었으면 true */
    public boolean cancel(long organizationId, UUID id, Instant now) {
        return jdbc.sql("""
                        UPDATE flow_replay_jobs SET status = 'CANCELLED', finished_at = :now
                         WHERE id = :id AND organization_id = :org AND status IN ('QUEUED','RUNNING')""")
                .param("now", Timestamp.from(now)).param("id", id).param("org", organizationId).update() == 1;
    }

    /** 내부 API는 작업 ID(UUID)로 찾는다 */
    @OrganizationScopeExempt("내부 API(API-FLW-13 조회)는 작업 ID(UUID)로 찾는다")
    public Optional<Job> find(UUID id) {
        return jdbc.sql("""
                        SELECT id, organization_id, flow_id, status, request::text, processed, total, result::text, error
                          FROM flow_replay_jobs WHERE id = :id""")
                .param("id", id).query((rs, n) -> job(rs)).optional();
    }

    @OrganizationScopeExempt("끝난 작업 정리(7일)는 조직과 무관한 시스템 보관 정책")
    public int deleteFinished(Instant before) {
        return jdbc.sql("DELETE FROM flow_replay_jobs WHERE status IN ('SUCCEEDED','FAILED','CANCELLED') AND created_at < :before")
                .param("before", Timestamp.from(before)).update();
    }

    private static Job job(java.sql.ResultSet rs) throws java.sql.SQLException {
        String result = rs.getString(8);
        long total = rs.getLong(7);
        return new Job(rs.getObject(1, UUID.class), rs.getLong(2), rs.getObject(3, UUID.class), rs.getString(4),
                Jsons.MAPPER.readTree(rs.getString(5)), rs.getLong(6), rs.wasNull() ? null : total,
                result == null ? null : Jsons.MAPPER.readTree(result), rs.getString(9));
    }
}
