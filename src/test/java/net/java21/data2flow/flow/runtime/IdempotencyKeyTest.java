package net.java21.data2flow.flow.runtime;

import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TC-FLW-090 BR-FLW-13 규칙 표: 행동은 아웃박스에 멱등 키 {@code sha256(flowId, nodeId, triggerMessageId[, 분할 인덱스])}로 기록하고 같은
 * 키는 한 번만 실행된다. <b>플로우 버전은 키에 넣지 않는다</b>(test-plan의 "version 포함" 문구는 BR-FLW-13에 맞춰 고침, 2026-10-04).
 */
class IdempotencyKeyTest {

    @ParameterizedTest(name = "{0}")
    @CsvSource({
            "같은 플로우·노드·메시지 → 같은 키, f-1, n-1, m-1, f-1, n-1, m-1, true",
            "메시지가 다르면 다른 키, f-1, n-1, m-1, f-1, n-1, m-2, false",
            "노드가 다르면 다른 키, f-1, n-1, m-1, f-1, n-2, m-1, false",
            "플로우가 다르면 다른 키, f-1, n-1, m-1, f-2, n-1, m-1, false"})
    @DisplayName("[FLW-05.01] TC-FLW-090 멱등 키 규칙 표")
    void keyTable(String name, String f1, String n1, String m1, String f2, String n2, String m2, boolean same) {
        assertThat(ActionIdempotencyKeys.flow(f1, n1, m1).equals(ActionIdempotencyKeys.flow(f2, n2, m2))).isEqualTo(same);
        assertThat(ActionIdempotencyKeys.flow(f1, n1, m1)).hasSize(64).matches("[0-9a-f]+");
    }

    @Test
    @DisplayName("[FLW-05.01] TC-FLW-090 같은 메시지를 v1과 v2(새 버전)로 다시 처리해도 같은 키라 아웃박스는 1건")
    void versionNotInKey() {
        FlowTestHarness v1 = FlowTestHarness.of(FlowFixtures.cooling(27, "PT5M", 24), (o, s, d) -> java.util.Set.of(), 1);
        FlowTestHarness v2 = FlowTestHarness.of(FlowFixtures.cooling(27, "PT5M", 24), (o, s, d) -> java.util.Set.of(), 2);
        var t = FlowFixtures.temperature(1, 30, v1.clock.instant());

        v1.send(t);
        v1.advance(Duration.ofMinutes(5));
        v2.send(t);
        v2.advance(Duration.ofMinutes(5));

        assertThat(v1.store.outbox()).hasSize(1);
        assertThat(v2.store.outbox()).hasSize(1);
        assertThat(v1.store.outbox().getFirst().action().idempotencyKey())
                .isEqualTo(v2.store.outbox().getFirst().action().idempotencyKey());
        assertThat(v1.store.outbox().getFirst().flowVersion()).isEqualTo(1);
        assertThat(v2.store.outbox().getFirst().flowVersion()).isEqualTo(2);
    }
}
