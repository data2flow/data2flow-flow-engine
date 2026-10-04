package net.java21.data2flow.flow.dryrun;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.IntegrationTestSupport;
import net.java21.data2flow.flow.support.TestInfrastructure;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** 시험 실행·과거 재생 내부 API 통합 시험(FLW-03.05·03.06, BR-FLW-11): 운영 상태(노드 상태·아웃박스)를 바꾸지 않는다 */
class DryRunIT extends IntegrationTestSupport {

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final MessageCodec CODEC = MessageCodec.create();

    @Autowired
    JdbcClient jdbc;
    @LocalServerPort
    int port;

    private HttpResponse<String> send(String method, String path, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).header("Content-Type",
                "application/json");
        b = body == null ? b.method(method, HttpRequest.BodyPublishers.noBody()) : b.method(method, HttpRequest.BodyPublishers.ofString(body));
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private long stateRows(UUID flow) {
        return jdbc.sql("SELECT count(*) FROM data2flow_flow.flow_node_state WHERE flow_id = :f").param("f", flow).query(Long.class).single()
                + jdbc.sql("SELECT count(*) FROM data2flow_flow.flow_outboxes WHERE flow_id = :f").param("f", flow).query(Long.class).single()
                + jdbc.sql("SELECT count(*) FROM data2flow_flow.flow_timers WHERE flow_id = :f").param("f", flow).query(Long.class).single();
    }

    @Test
    @DisplayName("[FLW-03.05][AT-FLW-05.1] TC-FLW-073 POST /internal/flow/test-runs: 28℃ → 추적에 드라이런 Thermostat.set, 운영 상태·아웃박스 0행")
    void testRun() throws Exception {
        UUID flow = UUID.randomUUID();
        ObjectNode body = Jsons.object();
        body.put("flowId", flow.toString()).put("organizationId", "1").put("version", 4);
        body.set("definition", Jsons.MAPPER.valueToTree(FlowFixtures.cooling(27, "PT5M", 24)));
        body.putObject("input").set("message", Jsons.MAPPER.readTree(CODEC.write(FlowFixtures.temperature(1, 28, Instant.now()))));

        HttpResponse<String> r = send("POST", "/internal/flow/test-runs", body.toString());

        assertThat(r.statusCode()).isEqualTo(200);
        JsonNode trace = Jsons.MAPPER.readTree(r.body()).path("response").path("trace");
        assertThat(trace.path("version").asInt()).isEqualTo(4);
        JsonNode last = trace.path("steps").get(trace.path("steps").size() - 1);
        assertThat(last.path("nodeId").asString()).isEqualTo("n-act00001");
        assertThat(last.path("action").path("dryRun").asBoolean()).isTrue();
        assertThat(stateRows(flow)).isZero();
        assertThat(send("POST", "/internal/flow/test-runs", "{\"input\":{\"rawMessageId\":1}}").statusCode()).isEqualTo(400);
    }

    @Test
    @DisplayName("[FLW-03.06][AT-FLW-05.2] TC-FLW-077 POST /internal/flow/replays → 202 작업 → core 과거 텔레메트리로 드라이런 → SUCCEEDED 요약, 운영 행동 0건")
    void replay() throws Exception {
        Instant from = Instant.parse("2026-02-20T00:00:00Z");
        List<CanonicalTelemetry> history = new ArrayList<>();
        for (int i = 0; i < 288; i++) {   // 하루, 5분 간격. 12:00~14:00 28℃
            Instant t = from.plus(Duration.ofMinutes(5L * i));
            int hour = t.atZone(java.time.ZoneOffset.UTC).getHour();
            history.add(FlowFixtures.temperature(1, hour >= 12 && hour < 14 ? 28 : 22, t));
        }
        TestInfrastructure.CORE.history(history);
        UUID flow = UUID.randomUUID();
        ObjectNode body = Jsons.object();
        body.put("flowId", flow.toString()).put("organizationId", "1").put("version", 2)
                .put("from", from.toString()).put("to", from.plus(Duration.ofDays(1)).toString());
        body.set("definition", Jsons.MAPPER.readTree("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["1"]}}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"temperature","op":">","value":27,"for":"PT10M","clear":26}},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"spaceId":31},"capability":"Thermostat","command":"set","args":{"mode":"cool"}}}],
                 "wires":[{"from":"n-trg00001","to":"n-thr00001"},{"from":"n-thr00001","port":"true","to":"n-act00001"}]}"""));

        HttpResponse<String> created = send("POST", "/internal/flow/replays", body.toString());
        assertThat(created.statusCode()).isEqualTo(202);
        String job = Jsons.MAPPER.readTree(created.body()).path("response").path("jobId").asString();

        await().atMost(Duration.ofSeconds(60)).until(() -> Jsons.MAPPER.readTree(send("GET", "/internal/flow/replays/" + job, null).body())
                .path("response").path("status").asString().equals("SUCCEEDED"));
        JsonNode result = Jsons.MAPPER.readTree(send("GET", "/internal/flow/replays/" + job, null).body()).path("response");
        assertThat(result.path("progress").path("processed").asLong()).isEqualTo(288);
        assertThat(result.path("result").path("actions").path("command").asLong()).isEqualTo(1);
        assertThat(stateRows(flow)).as("운영 상태·행동 0건").isZero();

        ObjectNode tooLong = body.deepCopy().put("to", from.plus(Duration.ofDays(8)).toString());
        HttpResponse<String> rejected = send("POST", "/internal/flow/replays", tooLong.toString());
        assertThat(rejected.statusCode()).isEqualTo(400);
        assertThat(rejected.body()).contains("FLOW_REPLAY_TOO_LARGE");
    }
}
