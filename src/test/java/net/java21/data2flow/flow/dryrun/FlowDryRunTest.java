package net.java21.data2flow.flow.dryrun;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.flow.FlowTrace;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.flow.common.DeploymentScope;
import net.java21.data2flow.flow.definition.service.CoreFlowDirectory;
import net.java21.data2flow.flow.dryrun.dto.ReplayRequest;
import net.java21.data2flow.flow.dryrun.dto.TestRunRequest;
import net.java21.data2flow.flow.dryrun.repository.ReplayJobRepository;
import net.java21.data2flow.flow.dryrun.service.ReplayService;
import net.java21.data2flow.flow.dryrun.service.TestRunService;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.domain.Overlay;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.domain.ActionGuard;
import net.java21.data2flow.flow.runtime.service.FlowExecutor;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import net.java21.data2flow.flow.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** 시험 실행·과거 재생(FLW-03.05·03.06, BR-FLW-11) */
class FlowDryRunTest {

    private static final MessageCodec CODEC = MessageCodec.create();
    private final MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
    private final FlowCompiler compiler = new FlowCompiler(FlowTestHarness.registry(SpaceDirectory.NONE));
    private final FlowExecutor executor = new FlowExecutor(clock, FlowTestHarness.LIMITS);
    private final FlowRegistry registry = new FlowRegistry();
    private final TestRunService testRuns = new TestRunService(compiler, registry, executor, ActionGuard.NONE, clock);

    private static ObjectNode input(CanonicalTelemetry t) {
        ObjectNode input = Jsons.object();
        input.set("message", Jsons.MAPPER.readTree(CODEC.write(t)));
        return input;
    }

    @Test
    @DisplayName("[FLW-03.05][AT-FLW-05.1] TC-FLW-073 28℃ 메시지로 시험 실행 → 제어 노드 \"드라이런: Thermostat.set(cool,24)\", 지속 타이머는 가상 시각으로 끝까지")
    void testRunCooling() {
        FlowTrace trace = testRuns.run(new TestRunRequest(FlowFixtures.FLOW.toString(), "1", 3, FlowFixtures.cooling(27, "PT5M", 24),
                input(FlowFixtures.temperature(1, 28, clock.instant())), null));

        assertThat(trace.version()).isEqualTo(3);
        assertThat(trace.hasDryRunActions()).isTrue();
        assertThat(trace.steps()).extracting(FlowTrace.Step::nodeId).containsSubsequence("n-trg00001", "n-agg00001", "n-thr00001",
                "n-thr00001", "n-act00001");
        FlowTrace.Step act = trace.steps().getLast();
        assertThat(act.action().dryRun()).isTrue();
        assertThat(act.inMs()).as("5분 지속 타이머 발화 뒤").isGreaterThanOrEqualTo(Duration.ofMinutes(5).toMillis());
    }

    @Test
    @DisplayName("[FLW-03.05] TC-FLW-074 BR-FLW-11 시험 실행은 행동을 기록만 하고 운영 상태(적재된 계획·저장소)를 바꾸지 않는다, startNodeId·body 입력, 잘못된 입력은 400")
    void testRunInputs() {
        FlowDefinition def = FlowFixtures.cooling(27, "PT0S", 24);
        var plan = compiler.compile(FlowFixtures.FLOW, 1, 5, def).plan();
        registry.put(new LoadedFlow(FlowFixtures.FLOW, 1, "f", "ACTIVE", plan, Overlay.NONE));

        ObjectNode body = Jsons.object();
        ObjectNode b = body.putObject("body");
        b.put("deviceId", 1).putObject("payload").put("temperature", 30);
        FlowTrace fromNode = testRuns.run(new TestRunRequest(FlowFixtures.FLOW.toString(), null, 5, null, body, "n-thr00001"));
        assertThat(fromNode.steps().getFirst().nodeId()).isEqualTo("n-thr00001");
        assertThat(fromNode.steps().getLast().action().dryRun()).isTrue();
        assertThat(plan.references().get()).as("적재된 계획을 잡지 않음").isZero();

        assertThatThrownBy(() -> testRuns.run(new TestRunRequest(null, null, null, null, Jsons.object().put("rawMessageId", 9), null)))
                .isInstanceOf(BusinessException.class).hasMessageContaining("FLOW_TEST_INPUT_INVALID");
        assertThatThrownBy(() -> testRuns.run(new TestRunRequest("not-uuid", null, null, def, body, null)))
                .isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> testRuns.run(new TestRunRequest(UUID.randomUUID().toString(), null, null, null, body, null)))
                .as("정의도 적재된 계획도 없음").isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> testRuns.run(new TestRunRequest(null, null, null, def, body, "n-none")))
                .isInstanceOf(BusinessException.class);
        FlowDefinition broken = FlowFixtures.definition("{\"schema\":\"data2flow.flow-definition/v1\",\"nodes\":[]}");
        assertThatThrownBy(() -> testRuns.run(new TestRunRequest(null, "1", null, broken, body, null)))
                .isInstanceOf(BusinessException.class).hasMessageContaining("FLOW_VALIDATION_FAILED");
        ObjectNode badMessage = Jsons.object();
        badMessage.putObject("message").put("v", 1);
        assertThatThrownBy(() -> testRuns.run(new TestRunRequest(null, "1", null, def, badMessage, null)))
                .isInstanceOf(BusinessException.class);
    }

    @Test
    @DisplayName("[FLW-03.05] 대상이 맞지 않는 텔레메트리도 첫 트리거부터 보여 준다")
    void unmatchedTelemetryStartsAtFirstTrigger() {
        FlowTrace trace = testRuns.run(new TestRunRequest(null, null, null, FlowFixtures.cooling(27, "PT0S", 24),
                input(FlowFixtures.telemetry(1, 999L, "temperature", 28, clock.instant())), null));
        assertThat(trace.steps().getFirst().nodeId()).isEqualTo("n-trg00001");
    }

    /** 7일 합성 시계열: 매일 13~15시 28℃(그 밖 22℃), 1·3·5일째 14시에는 31℃. 5분 간격 */
    private static List<CanonicalTelemetry> week(Instant start) {
        List<CanonicalTelemetry> out = new ArrayList<>();
        for (Instant t = start; t.isBefore(start.plus(Duration.ofDays(7))); t = t.plus(Duration.ofMinutes(5))) {
            long day = Duration.between(start, t).toDays();
            int hour = t.atZone(java.time.ZoneOffset.UTC).getHour();
            double v = hour >= 13 && hour < 15 ? 28 : 22;
            if (hour == 14 && (day == 0 || day == 2 || day == 4)) {
                v = 31;
            }
            out.add(FlowFixtures.temperature(1, v, t));
        }
        return out;
    }

    private static FlowDefinition coolingWithAlert() {
        return FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["1"]},"metrics":["temperature"]}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"temperature","op":">","value":27,"for":"PT10M","clear":26}},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"spaceId":31},"capability":"Thermostat","command":"set","args":{"mode":"cool","targetTemperature":24}}},
                  {"id":"n-thr00002","type":"condition.threshold","config":{"metric":"temperature","op":">","value":30,"clear":29}},
                  {"id":"n-ntf00001","type":"action.notify","config":{"channels":["TELEGRAM"]}}],
                 "wires":[{"from":"n-trg00001","to":"n-thr00001"},{"from":"n-thr00001","port":"true","to":"n-act00001"},
                          {"from":"n-trg00001","to":"n-thr00002"},{"from":"n-thr00002","port":"true","to":"n-ntf00001"}]}""");
    }

    @Test
    @DisplayName("[FLW-03.06][AT-FLW-05.2] TC-FLW-076 7일 재생 → \"제어 7회, 알림 3회\" 요약(지속 판정은 측정 시각), 실제 명령·알림 0건(메모리 저장소)")
    void replaySevenDays() {
        Instant from = Instant.parse("2026-02-23T00:00:00Z");
        List<CanonicalTelemetry> data = week(from);
        CoreFlowDirectory core = mock(CoreFlowDirectory.class);
        given(core.telemetryHistory(eq(1L), any(), eq(from), any(), any(), anyInt())).willAnswer(inv -> {
            String cursor = inv.getArgument(4);
            int start = cursor == null ? 0 : Integer.parseInt(cursor);
            int end = Math.min(data.size(), start + 1000);
            return new CoreFlowDirectory.TelemetryPage(data.subList(start, end), end < data.size() ? Integer.toString(end) : null,
                    (long) data.size());
        });
        ReplayJobRepository jobs = mock(ReplayJobRepository.class);
        DeploymentScope scope = new DeploymentScope();
        ReplayService replays = new ReplayService(jobs, compiler, executor, core, scope, "engine-1", clock);
        ReplayRequest request = new ReplayRequest(FlowFixtures.FLOW.toString(), "1", 4, coolingWithAlert(), from,
                from.plus(Duration.ofDays(7)), null);

        given(jobs.insert(eq(1L), eq(FlowFixtures.FLOW), any(), any())).willReturn(41L);
        long jobId = replays.submit(request);
        assertThat(jobId).isEqualTo(41L);
        ArgumentCaptor<JsonNode> stored = ArgumentCaptor.forClass(JsonNode.class);
        verify(jobs).insert(eq(1L), eq(FlowFixtures.FLOW), stored.capture(), any());
        assertThat(scope.organizations()).contains(1L);

        given(jobs.claim(any(), eq("engine-1"), any(), any())).willReturn(Optional.of(new ReplayJobRepository.Job(jobId, 1,
                FlowFixtures.FLOW, "RUNNING", stored.getValue(), 0, null, null, null)));
        given(jobs.progress(anyLong(), eq(jobId), anyLong(), any(), any())).willReturn(true);
        assertThat(replays.runNext(() -> false)).isTrue();

        ArgumentCaptor<JsonNode> result = ArgumentCaptor.forClass(JsonNode.class);
        verify(jobs).finish(eq(1L), eq(jobId), eq("SUCCEEDED"), result.capture(), any(), eq((long) data.size()), any());
        JsonNode r = result.getValue();
        assertThat(r.path("actions").path("command").asLong()).isEqualTo(7);
        assertThat(r.path("actions").path("notify").asLong()).isEqualTo(3);
        assertThat(r.path("executions").asLong()).isGreaterThanOrEqualTo(data.size());
        assertThat(r.path("branchCounts").path("n-thr00001").path("true").asLong()).isEqualTo(7);
        assertThat(r.path("errors").asLong()).isZero();
    }

    @Test
    @DisplayName("[FLW-03.06] 재생 기간 7일 초과는 FLOW_REPLAY_TOO_LARGE, from ≥ to·조직 없음·정의 없음은 400")
    void replayValidation() {
        ReplayService replays = new ReplayService(mock(ReplayJobRepository.class), compiler, executor, mock(CoreFlowDirectory.class),
                new DeploymentScope(), "engine-1", clock);
        Instant from = clock.instant();
        assertThatThrownBy(() -> replays.submit(new ReplayRequest(null, "1", 1, coolingWithAlert(), from, from.plus(Duration.ofDays(8)),
                null))).isInstanceOf(BusinessException.class).hasMessageContaining("FLOW_REPLAY_TOO_LARGE");
        assertThatThrownBy(() -> replays.submit(new ReplayRequest(null, "1", 1, coolingWithAlert(), from, from, null)))
                .isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> replays.submit(new ReplayRequest(null, null, 1, coolingWithAlert(), from, from.plusSeconds(1), null)))
                .isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> replays.submit(new ReplayRequest(null, "1", 1, null, from, from.plusSeconds(1), null)))
                .isInstanceOf(BusinessException.class);
        assertThat(replays.runNext(() -> false)).as("대기 작업 없음").isFalse();
    }

    @Test
    @DisplayName("[FLW-03.06] 재생 중 취소되면 결과 없이 멈추고, 데이터 조회 실패는 FAILED")
    void replayCancelAndFailure() {
        CoreFlowDirectory core = mock(CoreFlowDirectory.class);
        Instant from = clock.instant();
        given(core.telemetryHistory(anyLong(), any(), any(), any(), any(), anyInt()))
                .willReturn(new CoreFlowDirectory.TelemetryPage(week(from).subList(0, 10), "next", 20L));
        ReplayJobRepository jobs = mock(ReplayJobRepository.class);
        ReplayService replays = new ReplayService(jobs, compiler, executor, core, new DeploymentScope(), "engine-1", clock);
        JsonNode request = Jsons.MAPPER.valueToTree(new ReplayRequest(null, "1", 1, coolingWithAlert(), from, from.plusSeconds(3600),
                null));
        long id = 7;
        given(jobs.claim(any(), any(), any(), any())).willReturn(Optional.of(new ReplayJobRepository.Job(id, 1, new UUID(0, 0),
                "RUNNING", request, 0, null, null, null)));
        given(jobs.progress(anyLong(), anyLong(), anyLong(), any(), any())).willReturn(false);
        assertThat(replays.runNext(() -> false)).isTrue();
        verify(jobs, org.mockito.Mockito.never()).finish(anyLong(), anyLong(), eq("SUCCEEDED"), any(), any(), anyLong(), any());

        given(core.telemetryHistory(anyLong(), any(), any(), any(), any(), anyInt())).willThrow(new IllegalStateException("core 없음"));
        assertThat(replays.runNext(() -> false)).isTrue();
        verify(jobs).finish(eq(1L), eq(id), eq("FAILED"), any(), eq("core 없음"), anyLong(), any());
        given(jobs.find(id)).willReturn(Optional.of(new ReplayJobRepository.Job(id, 1, new UUID(0, 0), "FAILED", request, 10, 20L,
                null, "core 없음")));
        JsonNode view = ReplayService.view(replays.find(id).orElseThrow());
        assertThat(view.path("progress").path("total").asLong()).isEqualTo(20);
        assertThat(view.path("status").asString()).isEqualTo("FAILED");
        replays.cancel(id);
        verify(jobs).cancel(eq(1L), eq(id), any());
        replays.cleanup();
    }
}
