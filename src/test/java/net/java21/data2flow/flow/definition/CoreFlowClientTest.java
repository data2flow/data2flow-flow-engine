package net.java21.data2flow.flow.definition;

import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.common.TransientFailures;
import net.java21.data2flow.flow.definition.service.CoreFlowClient;
import net.java21.data2flow.flow.support.CoreApiStub;
import net.java21.data2flow.flow.support.FlowFixtures;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** core 내부 API 클라이언트: API-FLW-80·81, API-DEV-128 응답 해석, 204·404·5xx, X-CALLER-SERVICE(ADR-021) */
class CoreFlowClientTest {

    private final CoreApiStub core = new CoreApiStub();
    private final CoreFlowClient client = new CoreFlowClient(new FlowEngineProperties.Core(core.baseUrl(), Duration.ofSeconds(2),
            Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofMinutes(5)));

    @AfterEach
    void close() {
        core.close();
    }

    @Test
    @DisplayName("[FLW-01.06] API-FLW-80 목록(문자열 ID), sinceVersion이 같으면 204 → 빈 값, API-FLW-81 단건·404")
    void runtimeAndFlow() {
        UUID id = UUID.randomUUID();
        core.put(id, 7, 3, "ACTIVE", FlowFixtures.cooling(27, "PT5M", 24), List.of("n-act00001"), 5);

        var snapshot = client.runtime(null).orElseThrow();
        var flow = snapshot.flows().getFirst();

        assertThat(snapshot.organizationId()).isEqualTo(1);
        assertThat(flow.flowId()).isEqualTo(id);
        assertThat(flow.organizationId()).isEqualTo(7);
        assertThat(flow.activeVersion()).isEqualTo(3);
        assertThat(flow.definition().nodes()).hasSize(4);
        assertThat(flow.overlay().bypass()).containsExactly("n-act00001");
        assertThat(flow.overlay().revision()).isEqualTo(5);
        assertThat(flow.running()).isTrue();
        assertThat(client.runtime(snapshot.version())).isEmpty();
        assertThat(client.flow(id)).isPresent();
        assertThat(client.flow(UUID.randomUUID())).isEmpty();
        assertThat(core.calls()).allSatisfy(c -> assertThat(c).endsWith("data2flow-flow-engine"));
    }

    @Test
    @DisplayName("[FLW-05.01] API-DEV-128 공간 측정 기기, core 5xx는 일시 장애(CoreUnavailable)")
    void spacesAndFailure() {
        core.space(31, Set.of(101L, 102L));

        assertThat(client.measuringDevices(1, 31, true)).containsExactlyInAnyOrder(101L, 102L);
        assertThat(core.calls().getLast()).contains("relation=measures&includeDescendants=true");
        CoreFlowClient missingRoute = new CoreFlowClient(new FlowEngineProperties.Core(core.baseUrl() + "/none", Duration.ofSeconds(2),
                Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofMinutes(5)));
        assertThat(missingRoute.runtime(null)).as("경로가 없으면(404) 변경 없음으로 본다").isEmpty();
        core.failing(true);
        assertThatThrownBy(() -> client.runtime(null)).isInstanceOf(TransientFailures.CoreUnavailableException.class)
                .satisfies(e -> assertThat(TransientFailures.isTransient(e)).isTrue());
    }
}
