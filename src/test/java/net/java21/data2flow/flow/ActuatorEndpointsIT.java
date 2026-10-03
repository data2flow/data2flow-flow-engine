package net.java21.data2flow.flow;

import net.java21.data2flow.flow.support.IntegrationTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.web.server.LocalManagementPort;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 관리 포트(8081, 여기서는 임의 포트)에서 프로브와 지표가 나온다. readiness는 처음 동기화와 텔레메트리 소비자가 열려야 UP이다.
 * OPS-01(지표), NFR-06(무중단 배포의 프로브 전제)
 */
class ActuatorEndpointsIT extends IntegrationTestSupport {

    @LocalManagementPort
    int managementPort;

    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    @DisplayName("liveness·readiness 프로브가 UP(readiness에 flowRuntime: 동기화·스트림 소비자)")
    void probesAreUp() {
        await().atMost(Duration.ofSeconds(60)).ignoreExceptions().untilAsserted(() -> {
            for (String path : new String[]{"/actuator/health/liveness", "/actuator/health/readiness"}) {
                HttpResponse<String> res = get(path);
                assertThat(res.statusCode()).as(path).isEqualTo(200);
                assertThat(res.body()).as(path).contains("\"UP\"");
            }
        });
    }

    @Test
    @DisplayName("OPS-01 Prometheus 지표에 application 태그가 붙는다")
    void prometheusMetrics() throws Exception {
        HttpResponse<String> res = get("/actuator/prometheus");
        assertThat(res.statusCode()).isEqualTo(200);
        assertThat(res.body()).contains("application=\"data2flow-flow-engine\"");
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + managementPort + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
