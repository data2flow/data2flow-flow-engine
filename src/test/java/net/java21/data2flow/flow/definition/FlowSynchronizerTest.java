package net.java21.data2flow.flow.definition;

import net.java21.data2flow.flow.apply.service.ApplyReporter;
import net.java21.data2flow.flow.common.DeploymentScope;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.definition.service.CachedSpaceDirectory;
import net.java21.data2flow.flow.definition.service.CoreFlowClient;
import net.java21.data2flow.flow.definition.service.FlowSynchronizer;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.plan.domain.ExecutionPlan;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.repository.NodeStateRepository;
import net.java21.data2flow.flow.support.CoreApiStub;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import net.java21.data2flow.flow.support.MutableClock;
import net.java21.data2flow.flow.timer.repository.TimerRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * 라이브 리로드(live-reload §4, FLW-01.06 배포의 엔진 쪽): 컴파일 → 원자적 전환 → 보고, 컴파일 실패면 이전 버전 유지, 오버레이만 바뀌면
 * 계획 재사용, DISABLED·목록에서 빠지면 내리고 대기 타이머 취소(BR-FLW-18), 메시지는 시작할 때 읽은 계획 하나로(BR-FLW-06).
 */
class FlowSynchronizerTest {

    private final CoreApiStub core = new CoreApiStub();
    private final MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
    private final FlowRegistry registry = new FlowRegistry();
    private final ApplyReporter reporter = mock(ApplyReporter.class);
    private final NodeStateRepository states = mock(NodeStateRepository.class);
    private final TimerRepository timers = mock(TimerRepository.class);
    private final DeploymentScope scope = new DeploymentScope();
    private final CoreFlowClient client = new CoreFlowClient(new FlowEngineProperties.Core(core.baseUrl(), Duration.ofSeconds(2),
            Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofMinutes(5)));
    private final SpaceDirectory spaces = new CachedSpaceDirectory(client, Duration.ofMinutes(5), clock);
    private final FlowSynchronizer sync = new FlowSynchronizer(client, new FlowCompiler(FlowTestHarness.registry(spaces)), registry,
            reporter, states, timers, scope, Duration.ofHours(24), clock, null);
    private final UUID flow = UUID.randomUUID();

    @AfterEach
    void close() {
        core.close();
    }

    @Test
    @DisplayName("[FLW-01.06] v1 적용 → v2 적용: 원자적 전환, 이전 계획은 처리 중 메시지가 끝나야 드레인, 버전마다 적용 보고")
    void applyNewVersion() {
        core.put(flow, 1, 1, "ACTIVE", FlowFixtures.cooling(27, "PT5M", 24));
        sync.syncAll(true);
        ExecutionPlan v1 = registry.get(flow).orElseThrow().plan().acquire();   // v1로 처리 중인 메시지

        core.put(flow, 1, 2, "ACTIVE", FlowFixtures.cooling(28, "PT5M", 24));
        sync.syncFlow(flow);

        assertThat(registry.get(flow).orElseThrow().version()).isEqualTo(2);
        assertThat(v1.version()).as("메시지는 시작할 때 읽은 v1으로 끝까지(BR-FLW-06)").isEqualTo(1);
        assertThat(registry.sweep()).as("v1 드레인 대기").isEqualTo(1);
        v1.release();
        assertThat(registry.sweep()).isZero();
        verify(reporter).report(eq(1L), eq(flow), eq(1), eq(0L), anyLong(), isNull());
        verify(reporter).report(eq(1L), eq(flow), eq(2), eq(0L), anyLong(), isNull());
        verify(states, org.mockito.Mockito.times(2)).retainRemoved(eq(1L), eq(flow), any(), eq(clock.instant().plus(Duration.ofHours(24))));
        assertThat(scope.organizations()).containsExactly(1L);
    }

    @Test
    @DisplayName("[FLW-01.06] 컴파일 실패는 이전 버전을 유지하고 오류를 보고한다(EVT-FLW-02 error)")
    void compileFailureKeepsPrevious() {
        core.put(flow, 1, 1, "ACTIVE", FlowFixtures.cooling(27, "PT5M", 24));
        sync.syncAll(true);
        core.put(flow, 1, 2, "ACTIVE", FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[{"id":"n-x0000001","type":"transform.map","config":{}}]}"""));

        sync.syncFlow(flow);

        assertThat(registry.get(flow).orElseThrow().version()).isEqualTo(1);
        verify(reporter).report(eq(1L), eq(flow), eq(1), eq(0L), anyLong(), startsWith("nodes[n-x0000001].config.rules"));
    }

    @Test
    @DisplayName("[FLW-06.04] 오버레이만 바뀌면 계획을 다시 컴파일하지 않고 바꿔 끼운다, PAUSED는 적재하되 실행하지 않음")
    void overlayAndPause() {
        core.put(flow, 1, 1, "ACTIVE", FlowFixtures.cooling(27, "PT5M", 24));
        sync.syncAll(true);
        ExecutionPlan plan = registry.get(flow).orElseThrow().plan();

        core.put(flow, 1, 1, "PAUSED", FlowFixtures.cooling(27, "PT5M", 24), List.of("n-act00001"), 3);
        sync.syncFlow(flow);

        var loaded = registry.get(flow).orElseThrow();
        assertThat(loaded.plan()).isSameAs(plan);
        assertThat(loaded.overlay().bypass()).containsExactly("n-act00001");
        assertThat(loaded.running()).isFalse();
        assertThat(registry.running(1)).isEmpty();
    }

    @Test
    @DisplayName("[FLW-08.04] BR-FLW-18 DISABLED(404)·목록에서 빠지면 내리고 대기 타이머를 취소, 적용 행을 지운다")
    void unload() {
        core.put(flow, 1, 1, "ACTIVE", FlowFixtures.cooling(27, "PT5M", 24));
        UUID other = UUID.randomUUID();
        core.put(other, 1, 1, "ACTIVE", FlowFixtures.cooling(27, "PT5M", 24));
        sync.syncAll(true);

        core.remove(flow);
        sync.syncFlow(flow);
        core.remove(other);
        sync.syncAll(false);

        assertThat(registry.all()).isEmpty();
        verify(timers).cancelFlow(1, flow);
        verify(timers).cancelFlow(1, other);
        verify(reporter).withdraw(1, flow);
    }

    @Test
    @DisplayName("[FLW-01.06] 바뀐 것이 없으면(204) 다시 적용하지 않는다")
    void unchanged() {
        core.put(flow, 1, 1, "ACTIVE", FlowFixtures.cooling(27, "PT5M", 24));
        sync.syncAll(false);
        sync.syncAll(false);

        verify(reporter).report(anyLong(), any(), anyInt(), anyLong(), anyLong(), isNull());
        verify(timers, never()).cancelFlow(anyLong(), any());
        assertThat(sync.synced()).isTrue();
    }
}
