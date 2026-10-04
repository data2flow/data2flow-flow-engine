package net.java21.data2flow.flow.node;

import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static net.java21.data2flow.flow.support.FlowTestHarness.outputs;
import static net.java21.data2flow.flow.support.FlowTestHarness.ports;
import static org.assertj.core.api.Assertions.assertThat;

/** 규칙에 쓰는 조건 노드(RUL-01.02~01.07·01.12, FLW-02): 연속 횟수·무수신·변화율·시간·복합 */
class RuleConditionNodesTest {

    private static String body(double value, String metric, Instant at) {
        return """
                {"deviceId":1,"spaceId":31,"measuredAt":"%s","payload":{"%s":%s}}""".formatted(at, metric, value);
    }

    @Test
    @DisplayName("[RUL-01.12][AT-RUL-04.6] TC-RUL-034 repeat=3: 초과·초과·정상·초과·초과·초과 → 6번째에서 발생 1회, 정상 1회로 카운터 0")
    void repeatCountsConsecutive() {
        FlowTestHarness h = FlowTestHarness.node("condition.threshold",
                "{\"metric\":\"co2\",\"op\":\">\",\"value\":1000,\"repeat\":3,\"clear\":900}");
        double[] values = {1100, 1100, 950, 1100, 1100, 1100};
        List<String> fired = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            fired.add(String.join(",", ports(List.of(h.sendTo("n-test0001", body(values[i], "co2", h.clock.instant()))), "n-test0001")));
        }
        assertThat(fired).containsExactly("", "", "", "", "", "true");
    }

    @Test
    @DisplayName("[RUL-01.12] TC-RUL-033·034 BR-RUL-05 repeat=3 + for 5m: 3회 연속이 2분에 채워져도 5분 타이머 만기에 발생, 5분이 먼저 지나면 다음 참에서 횟수만 본다")
    void repeatWithDuration() {
        FlowTestHarness h = FlowTestHarness.node("condition.threshold",
                "{\"metric\":\"co2\",\"op\":\">\",\"value\":1000,\"repeat\":3,\"for\":\"PT5M\",\"clear\":900}");
        Instant t0 = h.clock.instant();
        h.sendTo("n-test0001", body(1100, "co2", t0));
        h.sendTo("n-test0001", body(1100, "co2", t0.plus(Duration.ofMinutes(1))));
        var third = h.sendTo("n-test0001", body(1100, "co2", t0.plus(Duration.ofMinutes(2))));
        assertThat(ports(List.of(third), "n-test0001")).isEmpty();
        var fired = h.advance(Duration.ofMinutes(5));
        assertThat(ports(fired, "n-test0001")).containsExactly("true");

        // 횟수가 모자란 채로 5분이 지나면 다음 참에서 횟수만 본다
        FlowTestHarness g = FlowTestHarness.node("condition.threshold",
                "{\"metric\":\"co2\",\"op\":\">\",\"value\":1000,\"repeat\":3,\"for\":\"PT5M\",\"clear\":900}");
        g.sendTo("n-test0001", body(1100, "co2", g.clock.instant()));
        assertThat(ports(g.advance(Duration.ofMinutes(5)), "n-test0001")).isEmpty();
        g.sendTo("n-test0001", body(1100, "co2", g.clock.instant()));
        var last = g.sendTo("n-test0001", body(1100, "co2", g.clock.instant()));
        assertThat(ports(List.of(last), "n-test0001")).containsExactly("true");
    }

    @Test
    @DisplayName("[RUL-01.05][FLW-02] TC-FLW-041·TC-RUL-016 무수신 30분: 29분 59초까지 0건, 30분에 timeout 1회(반복 없음), 다시 오면 restored 1회·타이머 재무장")
    void noData() {
        FlowTestHarness h = FlowTestHarness.node("condition.noData", "{\"window\":\"PT30M\"}");
        h.sendTo("n-test0001", body(25, "temperature", h.clock.instant()));
        assertThat(ports(h.advance(Duration.ofMinutes(29).plusSeconds(59)), "n-test0001")).isEmpty();
        var timeout = h.advance(Duration.ofSeconds(1));
        assertThat(ports(timeout, "n-test0001")).containsExactly("timeout");
        assertThat(outputs(timeout, "n-test0001").getFirst().payload().path("noData").path("window").asString()).isEqualTo("PT30M");
        assertThat(ports(h.advance(Duration.ofHours(2)), "n-test0001")).as("같은 무수신 구간에서 반복 없음").isEmpty();

        var restored = h.sendTo("n-test0001", body(25, "temperature", h.clock.instant()));
        assertThat(ports(List.of(restored), "n-test0001")).containsExactly("restored");
        assertThat(h.store.timers()).hasSize(1);
        // 메시지가 계속 오면 만기 때 마지막 수신 기준으로 다시 건다(발생 없음)
        h.advance(Duration.ofMinutes(20));
        h.sendTo("n-test0001", body(25, "temperature", h.clock.instant()));
        assertThat(ports(h.advance(Duration.ofMinutes(10)), "n-test0001")).isEmpty();
        assertThat(ports(h.advance(Duration.ofMinutes(20)), "n-test0001")).containsExactly("timeout");
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
            "22.0→25.5(10분) 상승 3 이상 true|up|22.0|25.5|true",
            "22.0→24.9 false|up|22.0|24.9|false",
            "하강 25→21은 up이라 false|up|25|21|false",
            "하강 25→21 down true|down|25|21|true",
            "any는 어느 쪽이든|any|25|21|true"})
    @DisplayName("[RUL-01.04][FLW-02] TC-FLW-040·TC-RUL-015 변화율 window=10m, delta=3")
    void rateOfChange(String name, String direction, double from, double to, boolean expected) {
        FlowTestHarness h = FlowTestHarness.node("condition.rateOfChange",
                "{\"metric\":\"temperature\",\"window\":\"PT10M\",\"delta\":3,\"direction\":\"" + direction + "\"}");
        Instant t0 = h.clock.instant();
        var first = h.sendTo("n-test0001", body(from, "temperature", t0));
        assertThat(ports(List.of(first), "n-test0001")).as("창 안 표본 1개면 판정 보류").isEmpty();
        var second = h.sendTo("n-test0001", body(to, "temperature", t0.plus(Duration.ofMinutes(10))));
        assertThat(ports(List.of(second), "n-test0001")).containsExactly(expected ? "true" : "false");
    }

    @Test
    @DisplayName("[RUL-01.04] 변화율 emit=change: 판정이 바뀔 때만 내보낸다(규칙 알람 발생·해제 한 번씩)")
    void rateOfChangeEdges() {
        FlowTestHarness h = FlowTestHarness.node("condition.rateOfChange",
                "{\"metric\":\"temperature\",\"window\":\"PT10M\",\"delta\":3,\"direction\":\"up\",\"emit\":\"change\"}");
        Instant t0 = h.clock.instant();
        List<String> out = new ArrayList<>();
        double[] values = {22, 23, 26, 27, 27.5, 27.5, 27.5, 27.5};
        for (int i = 0; i < values.length; i++) {
            out.addAll(ports(List.of(h.sendTo("n-test0001", body(values[i], "temperature", t0.plus(Duration.ofMinutes(2L * i))))),
                    "n-test0001"));
        }
        assertThat(out).containsExactly("true", "false");
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
            "월 08:59:59 false|2026-03-01T23:59:59Z|false",
            "월 09:00 true|2026-03-02T00:00:00Z|true",
            "월 17:59:59 true|2026-03-02T08:59:59Z|true",
            "월 18:00 false(끝 배타)|2026-03-02T09:00:00Z|false",
            "토 10:00 false|2026-03-07T01:00:00Z|false"})
    @DisplayName("[RUL-01.07][FLW-02] TC-FLW-043 시간 조건 days=MON-FRI 09:00~18:00 Asia/Seoul")
    void timeWindow(String name, String at, boolean expected) {
        FlowTestHarness h = FlowTestHarness.node("condition.timeWindow",
                "{\"days\":[\"MON\",\"TUE\",\"WED\",\"THU\",\"FRI\"],\"from\":\"09:00\",\"to\":\"18:00\"}");
        var r = h.sendTo("n-test0001", body(1, "magnet", Instant.parse(at)));
        assertThat(ports(List.of(r), "n-test0001")).containsExactly(expected ? "true" : "false");
    }

    @Test
    @DisplayName("[RUL-01.07][AT-RUL-04.4] TC-RUL-020 운영 시간 외(18:00~09:00, 자정 넘김): 일요일 03:00 문 열림 → true, 월 10:00 → false")
    void offHours() {
        FlowTestHarness h = FlowTestHarness.node("condition.timeWindow", "{\"from\":\"18:00\",\"to\":\"09:00\"}");
        assertThat(ports(List.of(h.sendTo("n-test0001", body(1, "magnet", Instant.parse("2026-03-07T18:00:00Z")))), "n-test0001"))
                .as("일 03:00 KST").containsExactly("true");
        assertThat(ports(List.of(h.sendTo("n-test0001", body(1, "magnet", Instant.parse("2026-03-02T01:00:00Z")))), "n-test0001"))
                .containsExactly("false");
        FlowTestHarness weekday = FlowTestHarness.node("condition.timeWindow",
                "{\"days\":[\"FRI\"],\"from\":\"22:00\",\"to\":\"06:00\"}");
        assertThat(ports(List.of(weekday.sendTo("n-test0001", body(1, "magnet", Instant.parse("2026-03-06T17:00:00Z")))),
                "n-test0001")).as("토 02:00 KST는 금요일 밤 구간").containsExactly("true");
        FlowTestHarness inverted = FlowTestHarness.node("condition.timeWindow", "{\"from\":\"09:00\",\"to\":\"18:00\",\"invert\":true}");
        assertThat(ports(List.of(inverted.sendTo("n-test0001", body(1, "magnet", Instant.parse("2026-03-07T18:00:00Z")))),
                "n-test0001")).containsExactly("true");
    }

    @Test
    @DisplayName("[RUL-01.06][AT-RUL-04.2] TC-RUL-017 복합 AND: co2 1,100·재실 0 → 발생 안 함, 재실 1이 따로 오면(마지막 값) 발생, emit=change")
    void groupAnd() {
        FlowTestHarness h = FlowTestHarness.node("condition.group",
                "{\"op\":\"AND\",\"emit\":\"change\",\"items\":[{\"metric\":\"co2\",\"op\":\">\",\"value\":1000},"
                        + "{\"metric\":\"occupancy\",\"op\":\"==\",\"value\":1}]}");
        var r1 = h.sendTo("n-test0001", "{\"deviceId\":1,\"payload\":{\"co2\":1100,\"occupancy\":0}}");
        assertThat(ports(List.of(r1), "n-test0001")).isEmpty();
        var r2 = h.sendTo("n-test0001", "{\"deviceId\":1,\"payload\":{\"occupancy\":1}}");
        assertThat(ports(List.of(r2), "n-test0001")).containsExactly("true");
        var r3 = h.sendTo("n-test0001", "{\"deviceId\":1,\"payload\":{\"co2\":800}}");
        assertThat(ports(List.of(r3), "n-test0001")).containsExactly("false");

        FlowTestHarness or = FlowTestHarness.node("condition.group",
                "{\"op\":\"OR\",\"items\":[{\"metric\":\"co2\",\"op\":\">\",\"value\":1000},{\"metric\":\"temperature\",\"op\":\"outside\",\"range\":[18,28]}]}");
        assertThat(ports(List.of(or.sendTo("n-test0001", "{\"deviceId\":1,\"payload\":{\"temperature\":29}}")), "n-test0001"))
                .containsExactly("true");
        assertThat(ports(List.of(or.sendTo("n-test0001", "{\"deviceId\":1,\"payload\":{\"humidity\":50}}")), "n-test0001"))
                .as("관심 항목이 없는 메시지는 판정하지 않음").isEmpty();
    }

    @Test
    @DisplayName("[RUL-01.06] 복합 조건 설정 오류: 항목 없음·연산 오류·range 누락")
    void groupInvalid() {
        for (String config : List.of("{\"op\":\"XOR\",\"items\":[{\"metric\":\"a\",\"op\":\">\",\"value\":1}]}",
                "{\"items\":[{\"metric\":\"a\",\"op\":\"~\",\"value\":1}]}", "{\"items\":[{\"metric\":\"a\",\"op\":\"inside\"}]}",
                "{\"items\":[{\"op\":\">\",\"value\":1}]}", "{\"items\":[{\"metric\":\"a\",\"op\":\">\"}]}")) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> FlowTestHarness.node("condition.group", config))
                    .as(config).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @DisplayName("[RUL-01.04][RUL-01.05][RUL-01.07] 조건 노드 설정 오류는 컴파일 오류(경로 포함)")
    void configErrors() {
        for (String[] c : List.of(new String[]{"condition.noData", "{\"window\":\"PT1S\"}"},
                new String[]{"condition.rateOfChange", "{\"metric\":\"t\",\"window\":\"PT10M\",\"delta\":0}"},
                new String[]{"condition.rateOfChange", "{\"metric\":\"t\",\"window\":\"PT10M\",\"delta\":1,\"direction\":\"left\"}"},
                new String[]{"condition.rateOfChange", "{\"metric\":\"t\",\"window\":\"PT48H\",\"delta\":1}"},
                new String[]{"condition.rateOfChange", "{\"metric\":\"t\",\"window\":\"PT1H\",\"delta\":1,\"emit\":\"sometimes\"}"},
                new String[]{"condition.timeWindow", "{\"days\":[\"XX\"]}"},
                new String[]{"condition.timeWindow", "{\"timezone\":\"Mars/Base\"}"},
                new String[]{"condition.timeWindow", "{\"spaceSchedule\":\"INSIDE\"}"},
                new String[]{"condition.threshold", "{\"metric\":\"t\",\"op\":\">\",\"value\":1,\"repeat\":0}"})) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> FlowTestHarness.node(c[0], c[1]))
                    .as(c[0] + " " + c[1]).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @DisplayName("[RUL-01.04] 변화율 값이 숫자가 아니면 error(TYPE_MISMATCH), 항목이 없으면 무시")
    void rateOfChangeTypes() {
        FlowTestHarness h = FlowTestHarness.node("condition.rateOfChange", "{\"metric\":\"temperature\",\"window\":\"PT10M\",\"delta\":1}");
        ExecutionReport r = h.sendTo("n-test0001", "{\"deviceId\":1,\"payload\":{\"temperature\":\"hot\"}}");
        assertThat(FlowTestHarness.errors(List.of(r), "n-test0001")).singleElement()
                .satisfies(s -> assertThat(s.errorType()).isEqualTo("TYPE_MISMATCH"));
        assertThat(ports(List.of(h.sendTo("n-test0001", "{\"deviceId\":1,\"payload\":{\"co2\":1}}")), "n-test0001")).isEmpty();
    }
}
