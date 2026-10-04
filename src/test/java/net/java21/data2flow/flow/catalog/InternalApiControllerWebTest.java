package net.java21.data2flow.flow.catalog;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.flow.apply.controller.InternalApplyController;
import net.java21.data2flow.flow.apply.dto.ApplyStatusResponse;
import net.java21.data2flow.flow.apply.service.ApplyStatusService;
import net.java21.data2flow.flow.catalog.controller.InternalCatalogController;
import net.java21.data2flow.flow.definition.service.FlowSynchronizer;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.plan.domain.FlowValidationError;
import net.java21.data2flow.flow.plan.service.NodeTypeRegistry;
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
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 내부 API(FLW-api §8): API-FLW-82 적용 상태, API-FLW-83 노드 카탈로그(TC-FLW-031의 원천), API-FLW-84 정의 검증 — 공통 응답 봉투 */
@WebMvcTest(controllers = {InternalCatalogController.class, InternalApplyController.class})
class InternalApiControllerWebTest {

    @TestConfiguration
    static class Beans {
        @Bean
        NodeTypeRegistry nodeTypeRegistry() {
            return FlowTestHarness.registry(SpaceDirectory.NONE);
        }
    }

    @Autowired
    MockMvc mvc;
    @MockitoBean
    FlowSynchronizer synchronizer;
    @MockitoBean
    ApplyStatusService applyStatus;

    @Test
    @DisplayName("[FLW-01.02] API-FLW-83 노드 카탈로그 9종(M3), 목록 봉투 {header, responses, totalCount}")
    void nodeTypes() throws Exception {
        mvc.perform(get("/internal/flow/node-types").header("X-CALLER-SERVICE", "data2flow-core-api"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.header.isSuccessful").value(true))
                .andExpect(jsonPath("$.totalCount").value(16))
                .andExpect(jsonPath("$.responses[0].type").value("trigger.telemetry"))
                .andExpect(jsonPath("$.responses[7].permissions[0]").value("FLOW_DEPLOY_CONTROL"));
    }

    @Test
    @DisplayName("[FLW-01.02] API-FLW-84 정의 검증: errors[{field, code, message}], 조직 ID 형식 오류는 400 INVALID_REQUEST")
    void validate() throws Exception {
        given(synchronizer.validate(any(), anyLong(), anyInt(), any())).willReturn(
                List.of(new FlowValidationError("nodes[n-thr00001].config.value", "INVALID_CONFIG", "기준값(value)이 필요합니다")));

        mvc.perform(post("/internal/flow/definitions/validate").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"organizationId\":\"1\",\"definition\":{\"schema\":\"data2flow.flow-definition/v1\",\"nodes\":[]}}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.response.errors[0].field").value("nodes[n-thr00001].config.value"))
                .andExpect(jsonPath("$.response.errors[0].code").value("INVALID_CONFIG"));
        mvc.perform(post("/internal/flow/definitions/validate").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"organizationId\":\"x\",\"definition\":{\"nodes\":[]}}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.header.resultCode").value("INVALID_REQUEST"));
    }

    @Test
    @DisplayName("[FLW-01.06] API-FLW-82 인스턴스별 적용 상태, 모르는 플로우는 404 RESOURCE_NOT_FOUND")
    void applyStatus() throws Exception {
        UUID flow = UUID.randomUUID();
        given(applyStatus.status(flow)).willReturn(new ApplyStatusResponse(flow.toString(),
                List.of(new ApplyStatusResponse.Instance("data2flow-flow-engine-0", 13, 2, Instant.parse("2026-03-02T00:00:00Z")))));
        given(applyStatus.status(new UUID(0, 1))).willThrow(new BusinessException(CommonErrorCode.RESOURCE_NOT_FOUND));

        mvc.perform(get("/internal/flow/flows/{id}/apply-status", flow))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.response.instances[0].appliedVersion").value(13));
        mvc.perform(get("/internal/flow/flows/{id}/apply-status", new UUID(0, 1)))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.header.resultCode").value("RESOURCE_NOT_FOUND"));
    }
}
