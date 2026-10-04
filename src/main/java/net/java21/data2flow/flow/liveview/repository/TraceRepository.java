package net.java21.data2flow.flow.liveview.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import net.java21.data2flow.flow.plan.domain.Jsons;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.JsonNode;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** {@code flow_traces}: 실행 추적(FLW-03.04, 1시간 보관). 어느 인스턴스가 처리했든 같은 DB에서 찾는다 */
@Repository
public class TraceRepository {

    private final JdbcClient jdbc;

    public TraceRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 저장할 추적 하나 */
    public record Row(long organizationId, UUID flowId, String messageId, int version, String trace, Instant expiresAt) {
    }

    public void insertAll(List<Row> rows, Instant now) {
        for (Row r : rows) {
            jdbc.sql("""
                            INSERT INTO flow_traces (organization_id, flow_id, message_id, flow_version, trace, created_at, expires_at)
                            VALUES (:org, :flow, :message, :version, CAST(:trace AS jsonb), :now, :expires)""")
                    .param("org", r.organizationId()).param("flow", r.flowId())
                    .param("message", r.messageId().length() > 64 ? r.messageId().substring(0, 64) : r.messageId())
                    .param("version", r.version()).param("trace", r.trace()).param("now", Timestamp.from(now))
                    .param("expires", Timestamp.from(r.expiresAt())).update();
        }
    }

    /**
     * 메시지의 추적(가장 최근). 내부 API는 조직 없이 메시지 ID(와 플로우 ID)로 찾는다.
     *
     * @param flowId 없으면 그 메시지를 처리한 플로우 중 아무거나(가장 최근)
     */
    @OrganizationScopeExempt("내부 API(API-FLW-41)는 메시지·플로우 ID(UUID)로 찾는다. 사용자 범위 검사는 core-api가 플로우 단위로 한다")
    public Optional<JsonNode> find(String messageId, UUID flowId, Instant now) {
        return jdbc.sql("""
                        SELECT trace::text FROM flow_traces
                         WHERE message_id = :message AND (CAST(:flow AS uuid) IS NULL OR flow_id = :flow) AND expires_at > :now
                         ORDER BY created_at DESC, id DESC LIMIT 1""")
                .param("message", messageId).param("flow", flowId).param("now", Timestamp.from(now))
                .query(String.class).optional().map(Jsons.MAPPER::readTree);
    }

    @OrganizationScopeExempt("보관 기한이 지난 추적 정리는 조직과 무관한 시스템 보관 정책")
    public int deleteExpired(Instant now) {
        return jdbc.sql("DELETE FROM flow_traces WHERE expires_at <= :now").param("now", Timestamp.from(now)).update();
    }
}
