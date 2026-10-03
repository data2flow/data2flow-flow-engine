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
