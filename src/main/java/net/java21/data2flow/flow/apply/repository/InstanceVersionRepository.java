package net.java21.data2flow.flow.apply.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * {@code flow_instance_versions}(ERD flow.md §3.6): 엔진 인스턴스별 적용 버전. EVT-FLW-02를 내기 전에 갱신하고, 편집기의 "모든 인스턴스
 * 적용됨"은 core-api가 API-FLW-82로 이 표를 읽어 판단한다. 보고가 10분 넘게 없는 인스턴스 행은 정리한다.
 */
@Repository
public class InstanceVersionRepository {

    private final JdbcClient jdbc;

    public InstanceVersionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 인스턴스 한 행 */
    public record InstanceVersion(String instanceId, int appliedVersion, long overlayRevision, Instant reportedAt) {
    }

    public void upsert(long organizationId, String instanceId, UUID flowId, int appliedVersion, long overlayRevision, Instant now) {
        jdbc.sql("""
                        INSERT INTO flow_instance_versions (instance_id, flow_id, organization_id, applied_version, overlay_revision, reported_at)
                        VALUES (:instance, :flow, :org, :version, :overlay, :now)
                        ON CONFLICT (instance_id, flow_id) DO UPDATE SET applied_version = EXCLUDED.applied_version,
                            overlay_revision = EXCLUDED.overlay_revision, reported_at = EXCLUDED.reported_at""")
                .param("instance", instanceId).param("flow", flowId).param("org", organizationId).param("version", appliedVersion)
                .param("overlay", (int) Math.min(Integer.MAX_VALUE, overlayRevision)).param("now", Timestamp.from(now)).update();
    }

    public void delete(long organizationId, String instanceId, UUID flowId) {
        jdbc.sql("DELETE FROM flow_instance_versions WHERE instance_id = :instance AND flow_id = :flow AND organization_id = :org")
                .param("instance", instanceId).param("flow", flowId).param("org", organizationId).update();
    }

    /** 살아 있는 인스턴스 보고 시각을 갱신한다(하트비트) */
    public void touch(String instanceId, long organizationId, Instant now) {
        jdbc.sql("UPDATE flow_instance_versions SET reported_at = :now WHERE instance_id = :instance AND organization_id = :org")
                .param("now", Timestamp.from(now)).param("instance", instanceId).param("org", organizationId).update();
    }

    /** 플로우의 인스턴스별 적용 버전(정리 기준보다 최근 보고만) */
    public List<InstanceVersion> findByFlow(long organizationId, UUID flowId, Instant reportedAfter) {
        return jdbc.sql("""
                        SELECT instance_id, applied_version, overlay_revision, reported_at FROM flow_instance_versions
                         WHERE flow_id = :flow AND organization_id = :org AND reported_at >= :after ORDER BY instance_id""")
                .param("flow", flowId).param("org", organizationId).param("after", Timestamp.from(reportedAfter))
                .query((rs, n) -> new InstanceVersion(rs.getString(1), rs.getInt(2), rs.getLong(3), rs.getTimestamp(4).toInstant()))
                .list();
    }

    /** 플로우의 조직(내부 조회 API가 조직 없이 flowId만 받으므로) */
    @OrganizationScopeExempt("내부 API(API-FLW-82)는 flowId로 찾고 조직은 행에서 읽는다")
    public java.util.Optional<Long> findOrganization(UUID flowId) {
        return jdbc.sql("SELECT organization_id FROM flow_instance_versions WHERE flow_id = :flow LIMIT 1")
                .param("flow", flowId).query(Long.class).optional();
    }

    @OrganizationScopeExempt("오래된 인스턴스 정리는 조직과 무관")
    public int deleteStale(Instant before) {
        return jdbc.sql("DELETE FROM flow_instance_versions WHERE reported_at < :before").param("before", Timestamp.from(before)).update();
    }
}
