package net.java21.data2flow.flow.plan;

import net.java21.data2flow.flow.plan.domain.ExecutionPlan;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.domain.Overlay;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 원자적 전환·버전 고정·참조 카운트 드레인(FLW-06.02, BR-FLW-06, design §4 ④⑥) */
class CompiledFlowRegistryTest {

    private static ExecutionPlan plan(int version) {
        return FlowTestHarness.of(FlowFixtures.cooling(27, "PT5M", 24), net.java21.data2flow.flow.node.service.SpaceDirectory.NONE,
                version).plan();
    }

    private static LoadedFlow loaded(ExecutionPlan plan) {
        return new LoadedFlow(FlowFixtures.FLOW, FlowFixtures.ORG, "냉방", "ACTIVE", plan, Overlay.NONE);
    }

    @Test
    @DisplayName("[FLW-06.02] TC-FLW-135 처리 중 메시지가 잡은 v12 계획은 교체 뒤에도 끝까지 쓰고, 참조 수가 0이 되면 닫힌다. 교체 뒤 시작한 메시지는 v13")
    void pinnedPlanSurvivesSwapAndDrains() {
        FlowRegistry registry = new FlowRegistry();
        ExecutionPlan v12 = plan(12);
        ExecutionPlan v13 = plan(13);
        registry.put(loaded(v12));

        LoadedFlow inFlight = registry.pin(FlowFixtures.FLOW).orElseThrow();
        registry.put(loaded(v13));                              // ④ 원자적 전환

        assertThat(inFlight.version()).isEqualTo(12);
        assertThat(v12.closed()).as("처리 중이라 닫히지 않음").isFalse();
        assertThat(registry.draining()).containsExactly(v12);
        LoadedFlow after = registry.pin(FlowFixtures.FLOW).orElseThrow();
        assertThat(after.version()).isEqualTo(13);

        inFlight.plan().release();                              // v12로 처리하던 메시지 끝
        assertThat(registry.sweep()).isZero();                  // ⑥ 드레인 → 닫힘
        assertThat(v12.closed()).isTrue();
        assertThat(v12.tryAcquire()).as("닫힌 계획은 잡을 수 없다").isFalse();
        assertThatThrownBy(v12::acquire).isInstanceOf(IllegalStateException.class);
        after.plan().release();
        assertThat(v13.closed()).isFalse();
    }

    @Test
    @DisplayName("[FLW-06.02] TC-FLW-133 BR-FLW-06: 잡으려던 계획이 방금 닫혔으면 새 계획을 다시 읽는다(닫힌 계획으로 처리되는 메시지 없음)")
    void pinRereadsWhenPlanClosed() {
        FlowRegistry registry = new FlowRegistry();
        ExecutionPlan v1 = plan(1);
        ExecutionPlan v2 = plan(2);
        registry.put(loaded(v1));
        registry.put(loaded(v2));
        assertThat(v1.closeIfDrained()).isTrue();
        assertThat(registry.pin(FlowFixtures.FLOW).orElseThrow().version()).isEqualTo(2);
        registry.remove(FlowFixtures.FLOW);
        assertThat(registry.pin(FlowFixtures.FLOW)).isEmpty();
    }

    @Test
    @DisplayName("[FLW-06.02] TC-FLW-133 적용 1,000회 중 4개 스레드가 메시지를 처리해도 각 메시지는 잡은 계획 하나로 처리되고 닫힌 계획은 쓰이지 않는다")
    void concurrentSwapNeverUsesClosedPlan() throws Exception {
        FlowRegistry registry = new FlowRegistry();
        registry.put(loaded(plan(1)));
        AtomicBoolean stop = new AtomicBoolean();
        AtomicBoolean violation = new AtomicBoolean();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch started = new CountDownLatch(4);
        List<Future<Integer>> workers = new ArrayList<>();
        for (int t = 0; t < 4; t++) {
            workers.add(pool.submit(() -> {
                started.countDown();
                int processed = 0;
                while (!stop.get()) {
                    LoadedFlow f = registry.pin(FlowFixtures.FLOW).orElseThrow();
                    int version = f.plan().version();
                    if (f.plan().closed()) {
                        violation.set(true);
                    }
                    processed++;
                    if (f.plan().version() != version) {
                        violation.set(true);
                    }
                    f.plan().release();
                }
                return processed;
            }));
        }
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
        for (int i = 2; i <= 1001; i++) {
            registry.put(loaded(plan(i)));   // 적용마다 새로 컴파일한 계획(운영과 같음)
        }
        stop.set(true);
        int total = 0;
        for (Future<Integer> w : workers) {
            total += w.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertThat(violation).isFalse();
        assertThat(total).isPositive();
        assertThat(registry.sweep()).isZero();
    }
}
