package net.java21.data2flow.flow.runtime.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import net.java21.data2flow.flow.plan.domain.Jsons;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.JsonNode;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;

/**
 * {@code flow_node_state}(ERD flow.md §3.1). 메시지 처리 트랜잭션 안에서 행을 잠그고 읽고 쓴다(BR-FLW-29). 행이 없으면 빈 행을 먼저
 * 만들어 잠근다: 다른 인스턴스(공간 단위 집계처럼 대상 키가 기기를 넘는 경우)와 동시에 처음 쓰는 경우에도 갱신을 잃지 않는다.
 */
@Repository
public class NodeStateRepository {

    private final JdbcClient jdbc;

    public NodeStateRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 잠그고 읽는다. 빈 상태면 null. 보관 중(삭제 노드)이던 행이면 살린다 */
    public JsonNode lock(long organizationId, UUID flowId, String nodeId, String targetKey) {
        jdbc.sql("""
                        INSERT INTO flow_node_state (flow_id, node_id, target_key, organization_id)
                        VALUES (:flow, :node, :key, :org) ON CONFLICT DO NOTHING""")
                .param("flow", flowId).param("node", nodeId).param("key", targetKey).param("org", organizationId).update();
        String state = jdbc.sql("""
                        SELECT state::text FROM flow_node_state
                         WHERE flow_id = :flow AND node_id = :node AND target_key = :key AND organization_id = :org FOR UPDATE""")
                .param("flow", flowId).param("node", nodeId).param("key", targetKey).param("org", organizationId)
                .query(String.class).optional().orElse("{}");
        JsonNode node = Jsons.MAPPER.readTree(state);
        return node.isEmpty() ? null : node;
    }

    public void update(long organizationId, UUID flowId, String nodeId, String targetKey, JsonNode state, Instant now) {
        jdbc.sql("""
                        UPDATE flow_node_state SET state = CAST(:state AS jsonb), updated_at = :now, retain_until = NULL
                         WHERE flow_id = :flow AND node_id = :node AND target_key = :key AND organization_id = :org""")
                .param("state", state == null ? "{}" : state.toString()).param("now", Timestamp.from(now))
                .param("flow", flowId).param("node", nodeId).param("key", targetKey).param("org", organizationId).update();
    }

    /** 읽기만(잠그지 않음, 시험·조회). 없으면 null */
    public JsonNode find(long organizationId, UUID flowId, String nodeId, String targetKey) {
        return jdbc.sql("""
                        SELECT state::text FROM flow_node_state
                         WHERE flow_id = :flow AND node_id = :node AND target_key = :key AND organization_id = :org""")
                .param("flow", flowId).param("node", nodeId).param("key", targetKey).param("org", organizationId)
                .query(String.class).optional().map(Jsons.MAPPER::readTree).filter(n -> !n.isEmpty()).orElse(null);
    }

    /**
     * 새 버전 적용 뒤(BR-FLW-07): 계획에 없는 노드의 상태는 보관 기한을 걸고(롤백하면 복원), 다시 생긴 노드의 상태는 살린다.
     */
    public void retainRemoved(long organizationId, UUID flowId, Collection<String> liveNodeIds, Instant retainUntil) {
        String[] live = liveNodeIds.toArray(String[]::new);
        jdbc.sql("""
                        UPDATE flow_node_state SET retain_until = :until
                         WHERE flow_id = :flow AND organization_id = :org AND retain_until IS NULL AND NOT (node_id = ANY(:live))""")
                .param("until", Timestamp.from(retainUntil)).param("flow", flowId).param("org", organizationId).param("live", live)
                .update();
        jdbc.sql("""
                        UPDATE flow_node_state SET retain_until = NULL
                         WHERE flow_id = :flow AND organization_id = :org AND retain_until IS NOT NULL AND node_id = ANY(:live)""")
                .param("flow", flowId).param("org", organizationId).param("live", live).update();
    }

    /** 보관 기한이 지난 상태를 지운다(모든 조직을 도는 정리 작업) */
    @OrganizationScopeExempt("보관 기한이 지난 행 정리는 조직과 무관한 시스템 보관 정책(ERD README §14)")
    public int deleteExpired(Instant now) {
        return jdbc.sql("DELETE FROM flow_node_state WHERE retain_until IS NOT NULL AND retain_until < :now")
                .param("now", Timestamp.from(now)).update();
    }
}
