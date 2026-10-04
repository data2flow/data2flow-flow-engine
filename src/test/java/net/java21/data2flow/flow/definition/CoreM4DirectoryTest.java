package net.java21.data2flow.flow.definition;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.definition.service.CachedSpaceDirectory;
import net.java21.data2flow.flow.definition.service.CoreFlowClient;
import net.java21.data2flow.flow.definition.service.CoreScriptDirectory;
import net.java21.data2flow.flow.guard.service.AutomationGuard;
import net.java21.data2flow.flow.plan.domain.ActionDraft;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.support.CoreApiStub;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** M4에서 엔진이 부르는 core 내부 조회(비상 정지·유지보수·공간 기기·스크립트 묶음·기기 태그·과거 텔레메트리) */
class CoreM4DirectoryTest {

    private final CoreApiStub core = new CoreApiStub();
    private final CoreFlowClient client = new CoreFlowClient(new FlowEngineProperties.Core(core.baseUrl(), Duration.ofSeconds(2),
            Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofMinutes(5)));

    @AfterEach
    void close() {
        core.close();
    }

    private static ActionDraft command(long deviceId) {
        return new ActionDraft("COMMAND", "data2flow.actions", "command", "k", Jsons.MAPPER.readTree(
                "{\"payload\":{\"target\":{\"deviceId\":" + deviceId + "}},\"source\":{}}"), "Switch.set");
    }

    @Test
    @DisplayName("[ACT-06.03][OPS-05.02] BR-FLW-19 비상 정지·유지보수를 core에서 다시 읽는다(시작·EMERGENCY_STOP 설정 변경·주기), 경로가 없으면(404) 없음")
    void guardRefresh() {
        core.emergencyStop(9, 1, 7L);
        core.space(7, Set.of(101L));
        core.maintenance(3, "DEVICE", 202, true);
        AutomationGuard guard = new AutomationGuard(client);

        guard.refresh();

        assertThat(guard.activeStops()).isEqualTo(1);
        assertThat(guard.skipReason(1, command(101), null)).contains("EMERGENCY_STOP");
        assertThat(guard.skipReason(2, command(101), null)).as("다른 조직").isEmpty();
        assertThat(guard.skipReason(1, command(202), null)).contains("MAINTENANCE");
        assertThat(guard.skipReason(1, command(303), null)).isEmpty();
        assertThat(guard.skipReason(1, new ActionDraft("NOTIFY", "x", "notify", "k", Jsons.object(), "n"), null))
                .as("알림은 막지 않음").isEmpty();

        core.clearEmergencyStops();
        core.emergencyStop(10, 1, null);
        guard.refresh();
        assertThat(guard.activeStops()).isEqualTo(1);
        FlowMessage trigger = new FlowMessage(Jsons.object().put("deviceId", 5), "m", "device:5");
        assertThat(guard.skipReason(1, command(999), trigger)).contains("EMERGENCY_STOP");

        AutomationGuard missing = new AutomationGuard(new CoreFlowClient(new FlowEngineProperties.Core(core.baseUrl() + "/none",
                Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofMinutes(5))));
        missing.refresh();
        assertThat(missing.activeStops()).isZero();
        core.failing(true);
        guard.refresh();
        assertThat(guard.activeStops()).as("읽기 실패면 지금 상태 유지").isEqualTo(1);
    }

    @Test
    @DisplayName("[FLW-02] scriptRef s-{id}@v{n}: 실행 묶음(API-SCR-32)의 활성 버전 코드, 다른 버전·없는 스크립트는 빈 값(30초 캐시)")
    void scripts() {
        core.script(12, 3, "return msg;");
        MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
        CoreScriptDirectory scripts = new CoreScriptDirectory(client, Duration.ofSeconds(30), clock);

        assertThat(scripts.code(1, 12, 3)).contains("return msg;");
        assertThat(scripts.code(1, 12, 2)).isEmpty();
        assertThat(scripts.code(1, 13, 1)).isEmpty();
        int calls = core.calls().size();
        scripts.code(1, 12, 3);
        assertThat(core.calls()).as("캐시").hasSize(calls);
        clock.advance(Duration.ofMinutes(1));
        scripts.invalidate();
        scripts.code(1, 12, 3);
        assertThat(core.calls()).hasSize(calls + 1);
    }

    @Test
    @DisplayName("[FLW-02] 트리거 태그 대상: 기기 태그(API-DEV-122 tags[])를 뒤에서 읽어 캐시, 공간(관계 무관) 기기 목록")
    void tagsAndSpaceDevices() {
        core.tags(15, List.of("lab", "floor-2"));
        core.space(31, Set.of(1L, 2L));
        CachedSpaceDirectory spaces = new CachedSpaceDirectory(client, Duration.ofMinutes(5), MutableClock.atUtc("2026-03-02T00:00:00Z"));

        assertThat(spaces.deviceTags(1, 15)).as("처음은 빈 값(뒤에서 읽음)").isEmpty();
        await().atMost(Duration.ofSeconds(5)).until(() -> spaces.deviceTags(1, 15).contains("lab"));
        spaces.invalidateAll();
        assertThat(spaces.deviceTags(1, 15)).contains("floor-2");
        assertThat(client.spaceDevices(1, 31, true)).containsExactlyInAnyOrder(1L, 2L);
        assertThat(core.calls().getLast()).doesNotContain("relation=");
        spaces.close();
    }

    @Test
    @DisplayName("[FLW-03.06] 과거 텔레메트리 쪽 단위 조회(측정 시각 순서, nextCursor·totalCount), core에 경로가 없으면 실패")
    void history() {
        Instant t0 = Instant.parse("2026-03-02T00:00:00Z");
        List<CanonicalTelemetry> items = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            items.add(FlowFixtures.temperature(1, 20 + i, t0.plus(Duration.ofMinutes(i))));
        }
        core.history(items);

        var first = client.telemetryHistory(1, List.of(1L), t0, t0.plus(Duration.ofHours(1)), null, 10);
        assertThat(first.items()).hasSize(10);
        assertThat(first.total()).isEqualTo(25);
        var last = client.telemetryHistory(1, List.of(), t0, t0.plus(Duration.ofHours(1)), "20", 10);
        assertThat(last.items()).hasSize(5);
        assertThat(last.nextCursor()).isNull();
        assertThat(core.calls().stream().anyMatch(c -> c.contains("deviceIds=1"))).isTrue();

        CoreFlowClient missing = new CoreFlowClient(new FlowEngineProperties.Core(core.baseUrl() + "/none", Duration.ofSeconds(2),
                Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofMinutes(5)));
        assertThatThrownBy(() -> missing.telemetryHistory(1, List.of(), t0, t0, null, 10)).isInstanceOf(IllegalStateException.class);
        assertThat(missing.scriptBundle(1)).isEmpty();
        assertThat(missing.deviceTags(1, 1)).isEmpty();
        assertThat(missing.spaceDevices(1, 1, true)).isEmpty();
        assertThat(missing.activeMaintenance()).isEmpty();
    }
}
