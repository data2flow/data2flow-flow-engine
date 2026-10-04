package net.java21.data2flow.flow.runtime.repository;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Array;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;

/**
 * {@code flow_partition_progress}(ERD flow.md §3.7, BR-FLW-29): (플로우, 스트림 파티션)별 처리 진행. 메시지 처리 트랜잭션 안에서 잠그고,
 * 다시 읽은 메시지(오프셋이 워터마크 이하이거나 먼저 끝난 목록에 있음)면 건너뛴다. 이 엔진은 파티션 안 메시지를 순서대로 처리하므로
 * 처리한 오프셋이 곧 워터마크이고 {@code processed_above}는 비어 있다(병렬 처리로 바꾸면 그 목록을 쓴다).
 */
@Repository
public class PartitionProgressRepository {

    private final JdbcClient jdbc;

    public PartitionProgressRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 잠그고 이미 처리한 오프셋인지 본다 */
    public boolean lockAndCheckProcessed(long organizationId, UUID flowId, int partition, long offset) {
        jdbc.sql("""
                        INSERT INTO flow_partition_progress (flow_id, stream_partition, organization_id)
                        VALUES (:flow, :partition, :org) ON CONFLICT DO NOTHING""")
                .param("flow", flowId).param("partition", partition).param("org", organizationId).update();
        return jdbc.sql("""
                        SELECT watermark_offset, processed_above FROM flow_partition_progress
                         WHERE flow_id = :flow AND stream_partition = :partition AND organization_id = :org FOR UPDATE""")
                .param("flow", flowId).param("partition", partition).param("org", organizationId)
                .query((rs, n) -> {
                    long watermark = rs.getLong(1);
                    Array above = rs.getArray(2);
                    Long[] values = above == null ? new Long[0] : (Long[]) above.getArray();
                    return offset <= watermark || Arrays.asList(values).contains(offset);
                }).optional().orElse(false);
    }

    /** 워터마크를 잠그고 읽는다(묶음 처리: 이미 처리한 메시지를 걸러 냄). 행이 없으면 -1 */
    public long lockWatermark(long organizationId, UUID flowId, int partition) {
        return jdbc.sql("""
                        SELECT watermark_offset FROM flow_partition_progress
                         WHERE flow_id = :flow AND stream_partition = :partition AND organization_id = :org FOR UPDATE""")
                .param("flow", flowId).param("partition", partition).param("org", organizationId)
                .query(Long.class).optional().orElse(-1L);
    }

    /**
     * 처리 진행을 이 오프셋으로 올린다(메시지 처리 트랜잭션의 마지막 문장, BR-FLW-29). 이미 이 오프셋 이상을 처리했으면 0을 돌려주고, 호출하는
     * 쪽은 트랜잭션을 되돌린다(다시 읽은 메시지의 노드 상태·행동을 반영하지 않음). 행이 없으면 만든다. 문장 하나라서 메시지마다 DB 왕복이
     * 잠금·확인·갱신 셋보다 적다(TC-FLW-134 초당 200건). 같은 오프셋을 두 소비자가 동시에 처리하면(장애 전환 직후) 나중 쪽은 이 문장에서
     * 먼저 쪽의 커밋을 기다린 뒤 0을 받는다.
     *
     * @return 올렸으면 1, 이미 처리한 오프셋이면 0
     */
    public int advance(long organizationId, UUID flowId, int partition, long offset, Instant now) {
        return jdbc.sql("""
                        INSERT INTO flow_partition_progress (flow_id, stream_partition, organization_id, watermark_offset, updated_at)
                        VALUES (:flow, :partition, :org, :offset, :now)
                        ON CONFLICT (flow_id, stream_partition) DO UPDATE
                           SET watermark_offset = EXCLUDED.watermark_offset, updated_at = EXCLUDED.updated_at
                         WHERE flow_partition_progress.watermark_offset < EXCLUDED.watermark_offset
                           AND flow_partition_progress.organization_id = EXCLUDED.organization_id""")
                .param("flow", flowId).param("partition", partition).param("org", organizationId).param("offset", offset)
                .param("now", Timestamp.from(now)).update();
    }

    public void markProcessed(long organizationId, UUID flowId, int partition, long offset, Instant now) {
        jdbc.sql("""
                        UPDATE flow_partition_progress
                           SET watermark_offset = GREATEST(watermark_offset, :offset),
                               processed_above = ARRAY(SELECT x FROM unnest(processed_above) x WHERE x > GREATEST(watermark_offset, :offset)),
                               updated_at = :now
                         WHERE flow_id = :flow AND stream_partition = :partition AND organization_id = :org""")
                .param("offset", offset).param("now", Timestamp.from(now)).param("flow", flowId).param("partition", partition)
                .param("org", organizationId).update();
    }
}
