package net.java21.data2flow.flow.catalog;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.flow.FlowTrace;
import net.java21.data2flow.flow.common.FlowEngineErrorCode;
import net.java21.data2flow.flow.dryrun.controller.InternalDryRunController;
import net.java21.data2flow.flow.dryrun.repository.ReplayJobRepository;
import net.java21.data2flow.flow.dryrun.service.ReplayService;
import net.java21.data2flow.flow.dryrun.service.TestRunService;
import net.java21.data2flow.flow.liveview.service.TraceQueryService;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.rule.controller.InternalRuleController;
import net.java21.data2flow.flow.rule.service.RuleFlowCompiler;
import net.java21.data2flow.flow.runtime.controller.InternalRuntimeController;
import net.java21.data2flow.flow.runtime.dto.FlowMetricsResponse;
import net.java21.data2flow.flow.runtime.service.FlowMetricsService;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * M4 내부 API(FLW-api §2·§8, RUL-01.01): API-FLW-14 지표, API-FLW-41 추적, API-FLW-12 시험 실행, API-FLW-13 재생, 규칙 컴파일 — 공통 응답 봉투·
 * 오류 코드.
 */
@WebMvcTest(controllers = {InternalRuntimeController.class, InternalDryRunController.class, InternalRuleController.class})
class InternalM4ControllerWebTest {

    @TestConfiguration
    static class Beans {
        @Bean
        RuleFlowCompiler ruleFlowCompiler() {
            return new RuleFlowCompiler();
        }

        @Bean
        FlowCompiler flowCompiler() {
            return new FlowCompiler(FlowTestHarness.registry(SpaceDirectory.NONE));
        }
    }

    @Autowired
    MockMvc mvc;
    @MockitoBean
    FlowMetricsService metrics;
    @MockitoBean
    TraceQueryService traces;
    @MockitoBean
    TestRunService testRuns;
    @MockitoBean
    ReplayService replays;

    @Test
    @DisplayName("[FLW-05.05] API-FLW-14 내부 지표 GET /internal/flow/flows/{flow-id}/metrics: 외부와 같은 모양(core가 그대로 중계), notify 필드 이름")
    void metrics() throws Exception {
        given(metrics.metrics(eq(FlowFixtures.FLOW), eq("1h"), eq(null))).willReturn(new FlowMetricsResponse(
                new FlowMetricsResponse.Summary(1000, 20, 0.02, 12.5, 50, new FlowMetricsResponse.Actions(10, 3, 5), 2),
                List.of(new FlowMetricsResponse.Node("n-thr00001", 1000, 20, 1.2)),
                List.of(new FlowMetricsResponse.Point(Instant.parse("2026-03-02T00:00:00Z"), 100, 2))));

        mvc.perform(get("/internal/flow/flows/" + FlowFixtures.FLOW + "/metrics").param("window", "1h"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.response.summary.executions").value(1000))
                .andExpect(jsonPath("$.response.summary.errorRate").value(0.02))
                .andExpect(jsonPath("$.response.summary.p95Ms").value(50.0))
                .andExpect(jsonPath("$.response.summary.actions.notify").value(3))
                .andExpect(jsonPath("$.response.nodes[0].nodeId").value("n-thr00001"))
                .andExpect(jsonPath("$.response.series[0].t").value("2026-03-02T00:00:00Z"));
    }

    @Test
    @DisplayName("[FLW-03.04] API-FLW-41 GET /internal/flow/traces/{message-id}?flowId= → Trace, 보관하지 않았거나 1시간 지난 추적은 404")
    void traces() throws Exception {
        given(traces.find("m-1", FlowFixtures.FLOW)).willReturn(Optional.of(Jsons.MAPPER.readTree(
                "{\"messageId\":\"m-1\",\"flowId\":\"" + FlowFixtures.FLOW + "\",\"version\":13,\"steps\":[],\"result\":\"COMPLETED\"}")));
        mvc.perform(get("/internal/flow/traces/m-1").param("flowId", FlowFixtures.FLOW.toString()))
                .andExpect(status().isOk()).andExpect(jsonPath("$.response.version").value(13));
        given(traces.find(eq("m-2"), any())).willReturn(Optional.empty());
        mvc.perform(get("/internal/flow/traces/m-2")).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.header.resultCode").value("RESOURCE_NOT_FOUND"));
    }

    @Test
    @DisplayName("[FLW-03.05] API-FLW-12 POST /internal/flow/test-runs → {trace}, 입력 오류는 400 FLOW_TEST_INPUT_INVALID")
    void testRun() throws Exception {
        given(testRuns.run(any())).willReturn(new FlowTrace("m-1", FlowFixtures.FLOW.toString(), 3, Instant.parse("2026-03-02T00:00:00Z"),
                List.of(), FlowTrace.COMPLETED, null));
        mvc.perform(post("/internal/flow/test-runs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"flowId\":\"" + FlowFixtures.FLOW + "\",\"input\":{\"body\":{\"payload\":{}}}}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.response.trace.version").value(3));

        given(testRuns.run(any())).willThrow(new BusinessException(FlowEngineErrorCode.FLOW_TEST_INPUT_INVALID,
                List.of(new FieldErrorDetail("input", "INVALID", "input이 필요합니다"))));
        mvc.perform(post("/internal/flow/test-runs").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.header.resultCode").value("FLOW_TEST_INPUT_INVALID"));
    }

    @Test
    @DisplayName("[FLW-03.06] API-FLW-13 POST /internal/flow/replays → 202 {jobId, status:QUEUED}, 조회·취소, 없는 작업은 404")
    void replays() throws Exception {
        UUID job = UUID.randomUUID();
        given(replays.submit(any())).willReturn(job);
        mvc.perform(post("/internal/flow/replays").contentType(MediaType.APPLICATION_JSON).content("{\"organizationId\":\"1\"}"))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.response.jobId").value(job.toString()))
                .andExpect(jsonPath("$.response.status").value("QUEUED"));

        ReplayJobRepository.Job row = new ReplayJobRepository.Job(job, 1, FlowFixtures.FLOW, "SUCCEEDED", Jsons.object(), 2016, 2016L,
                Jsons.MAPPER.readTree("{\"executions\":2016,\"actions\":{\"command\":7,\"notify\":3,\"sink\":0}}"), null);
        given(replays.find(job)).willReturn(Optional.of(row));
        mvc.perform(get("/internal/flow/replays/" + job)).andExpect(status().isOk())
                .andExpect(jsonPath("$.response.progress.processed").value(2016))
                .andExpect(jsonPath("$.response.result.actions.command").value(7));
        mvc.perform(post("/internal/flow/replays/" + job + "/cancel")).andExpect(status().isOk());
        mvc.perform(get("/internal/flow/replays/" + UUID.randomUUID())).andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("[RUL-01.01] 규칙 컴파일 POST /internal/flow/rules/compile → {definition, target}, 바꿀 수 없는 조건은 400 RULE_CONDITION_INVALID(errors[])")
    void ruleCompile() throws Exception {
        mvc.perform(post("/internal/flow/rules/compile").contentType(MediaType.APPLICATION_JSON).content("""
                        {"organizationId":"1","ruleId":"42","rule":{"scope":{"type":"DEVICE","ids":["7"]},
                         "condition":{"kind":"threshold","metric":"co2","op":">","value":1000,"for":"PT5M","clear":900},
                         "severity":"MAJOR","titleTemplate":"고CO2"}}"""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.response.target").value("DEVICE"))
                .andExpect(jsonPath("$.response.definition.nodes[0].id").value("n-rtrg0001"));
        mvc.perform(post("/internal/flow/rules/compile").contentType(MediaType.APPLICATION_JSON).content("""
                        {"organizationId":"1","ruleId":"42","rule":{"scope":{"type":"DEVICE","ids":["7"]},
                         "condition":{"kind":"anomaly"},"severity":"MAJOR","titleTemplate":"x"}}"""))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.header.resultCode").value("RULE_CONDITION_INVALID"))
                .andExpect(jsonPath("$.errors[0].field").value("rule.condition.kind"));
        mvc.perform(post("/internal/flow/rules/compile").contentType(MediaType.APPLICATION_JSON).content("{\"ruleId\":\"x\"}"))
                .andExpect(status().isBadRequest());
    }
}
