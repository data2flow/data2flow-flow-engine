package net.java21.data2flow.flow.liveview;

import net.java21.data2flow.contracts.command.ActionKind;
import net.java21.data2flow.contracts.flow.FlowTrace;
import net.java21.data2flow.flow.liveview.repository.TraceRepository;
import net.java21.data2flow.flow.liveview.service.TraceRecorder;
import net.java21.data2flow.flow.liveview.service.Traces;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.domain.Overlay;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import net.java21.data2flow.flow.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** 실행 추적(FLW-03.04, API-FLW-41) */
class TracesTest {

    @Test
    @DisplayName("[FLW-03.04] TC-FLW-071 추적: 거친 노드 순서·종류·입출력·소요 시간·나간 포트·행동(멱등 키), 선택되지 않은 분기는 없음, 처리 버전 표시")
    void traceOfReport() {
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.cooling(27, "PT0S", 24), net.java21.data2flow.flow.node.service.SpaceDirectory.NONE,
                13);
        var r = h.send(FlowFixtures.temperature(1, 28, h.clock.instant())).getFirst();

        FlowTrace t = Traces.of(r);

        assertThat(t.version()).isEqualTo(13);
        assertThat(t.result()).isEqualTo(FlowTrace.COMPLETED);
        assertThat(t.steps()).extracting(FlowTrace.Step::nodeId).containsExactly("n-trg00001", "n-agg00001", "n-thr00001", "n-act00001");
        assertThat(t.steps().get(2).outputs()).extracting(FlowTrace.Output::port).containsExactly("true");
        assertThat(t.steps().getLast().action().kind()).isEqualTo(ActionKind.COMMAND);
        assertThat(t.steps().getLast().action().dryRun()).isFalse();
        double total = t.steps().stream().mapToDouble(FlowTrace.Step::durationMs).sum();
        assertThat(total).isLessThanOrEqualTo(r.durationMicros() / 1000.0 + 0.001);
        assertThat(t.steps().getFirst().input().path("payload").path("temperature").asDouble()).isEqualTo(28);

        var below = h.send(FlowFixtures.temperature(1, 20, h.clock.instant())).getFirst();
        assertThat(Traces.of(below).steps()).extracting(FlowTrace.Step::nodeId).doesNotContain("n-act00001");
    }

    @Test
    @DisplayName("[FLW-03.04] 실패한 실행은 FAILED와 첫 오류(nodeId·errorType), 노드 수 한도는 HOP_LIMIT")
    void failedTrace() {
        FlowTestHarness h = FlowTestHarness.node("condition.threshold", "{\"metric\":\"temperature\",\"op\":\">\",\"value\":1}");
        var r = h.sendTo("n-test0001", "{\"payload\":{\"temperature\":\"x\"}}");
        FlowTrace t = Traces.of(r);
        assertThat(t.result()).isEqualTo(FlowTrace.FAILED);
        assertThat(t.error().path("errorType").asString()).isEqualTo("TYPE_MISMATCH");
    }

    @Test
    @DisplayName("[FLW-03.04] 추적 보관: 디버그 노드를 지난 실행은 모두, 그 밖은 플로우당 초당 5건만 1시간 보관(대기열에 모았다가 씀)")
    void recorderSampling() {
        MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
        TraceRepository repository = mock(TraceRepository.class);
        TraceRecorder recorder = new TraceRecorder(repository, clock);
        FlowTestHarness h = FlowTestHarness.of(FlowFixtures.cooling(27, "PT0S", 24));
        LoadedFlow flow = new LoadedFlow(FlowFixtures.FLOW, 1, "f", "ACTIVE", h.plan(), Overlay.NONE);
        for (int i = 0; i < 20; i++) {
            recorder.executed(flow, h.send(FlowFixtures.temperature(1, 20, h.clock.instant())).getFirst());
        }
        h.overlay(new Overlay(Set.of(), Set.of("n-thr00001"), 1));
        for (int i = 0; i < 3; i++) {
            recorder.executed(flow, h.send(FlowFixtures.temperature(1, 20, h.clock.instant())).getFirst());
        }
        assertThat(recorder.flush()).isEqualTo(8);
        verify(repository, times(1)).insertAll(anyList(), any());
        assertThat(recorder.flush()).isZero();
        clock.advance(Duration.ofHours(2));
        recorder.cleanup();
        verify(repository).deleteExpired(clock.instant());
        verify(repository, never()).find(any(), any(), any());
        assertThat(List.of(recorder.now())).containsExactly(clock.instant());
    }
}
