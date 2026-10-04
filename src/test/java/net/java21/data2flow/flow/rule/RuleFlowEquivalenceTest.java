package net.java21.data2flow.flow.rule;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.AlarmSignal;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.rule.service.RuleFlowCompiler;
import net.java21.data2flow.flow.runtime.domain.InMemoryExecutionStore;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 규칙 엔진 동등성(TC-RUL-003, BR-RUL-01·03·04·05, ADR-005): 규칙 템플릿 7종 × 7일 합성 시계열(seed 42, 5분 간격)에서 참조 평가기
 * {@link Reference}와 컴파일된 플로우(FlowTestHarness)의 알람 신호(RAISE·CLEAR, 시각) 순서가 완전히 같은지 본다. 템플릿 값은 API-RUL-05 기본
 * 템플릿 키의 대표값이다(정본은 core-api).
 */
class RuleFlowEquivalenceTest {

    private static final MessageCodec CODEC = MessageCodec.create();
    private static final Instant START = Instant.parse("2026-03-02T00:00:00Z");
    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");

    /** 템플릿: 키, 규칙 JSON, 측정 항목, 시계열 생성기 */
    record Template(String key, String rule, String metric, double base, double amplitude, double spike) {
    }

    static Stream<Arguments> templates() {
        String device = "{\"type\":\"DEVICE\",\"ids\":[\"1\"]}";
        return Stream.of(
                Arguments.of(new Template("high-co2", RuleFlowCompilerTest.rule(device,
                        "{\"kind\":\"threshold\",\"metric\":\"co2\",\"op\":\">\",\"value\":1000,\"for\":\"PT5M\",\"clear\":900}", ""),
                        "co2", 800, 250, 300)),
                Arguments.of(new Template("high-temp", RuleFlowCompilerTest.rule(device,
                        "{\"kind\":\"threshold\",\"metric\":\"temperature\",\"op\":\">\",\"value\":28,\"for\":\"PT10M\",\"clear\":27}", ""),
                        "temperature", 25, 3, 2)),
                Arguments.of(new Template("low-temp", RuleFlowCompilerTest.rule(device,
                        "{\"kind\":\"threshold\",\"metric\":\"temperature\",\"op\":\"<\",\"value\":18,\"for\":\"PT10M\",\"clear\":19}", ""),
                        "temperature", 21, 3, -2)),
                Arguments.of(new Template("high-humidity", RuleFlowCompilerTest.rule(device,
                        "{\"kind\":\"threshold\",\"metric\":\"humidity\",\"op\":\">\",\"value\":70,\"for\":\"PT15M\",\"clear\":65}", ""),
                        "humidity", 60, 10, 8)),
                Arguments.of(new Template("low-battery", RuleFlowCompilerTest.rule(device,
                        "{\"kind\":\"threshold\",\"metric\":\"battery\",\"op\":\"<\",\"value\":20,\"clear\":25,\"repeat\":2}", ""),
                        "battery", 30, 12, -5)),
                Arguments.of(new Template("no-data-30m", RuleFlowCompilerTest.rule(device, "{\"kind\":\"noData\",\"window\":\"PT30M\"}", ""),
                        "temperature", 22, 1, 0)),
                Arguments.of(new Template("door-open-off-hours", RuleFlowCompilerTest.rule(device,
                        "{\"kind\":\"threshold\",\"metric\":\"magnet\",\"op\":\"==\",\"value\":1}",
                        ",\"timeCondition\":{\"from\":\"18:00\",\"to\":\"09:00\"}"), "magnet", 0, 0, 0)));
    }

    /** 합성 시계열(seed 42): 5분 간격, 사인 + 잡음 + 가끔 튐. 무수신 템플릿은 가끔 40~90분 비움, 문은 가끔 열림(1) */
    static List<CanonicalTelemetry> series(Template t) {
        Random random = new Random(42);
        List<CanonicalTelemetry> out = new ArrayList<>();
        Instant at = START;
        Instant end = START.plus(Duration.ofDays(7));
        while (at.isBefore(end)) {
            double hours = Duration.between(START, at).toMinutes() / 60.0;
            double v;
            if (t.metric().equals("magnet")) {
                v = random.nextDouble() < 0.08 ? 1 : 0;
            } else {
                v = t.base() + t.amplitude() * Math.sin(hours / 24 * 2 * Math.PI) + random.nextGaussian() * t.amplitude() * 0.2
                        + (random.nextDouble() < 0.05 ? t.spike() : 0);
                v = Math.round(v * 10) / 10.0;
            }
            out.add(CanonicalTelemetry.builder().organizationId(1).sourceId(3).externalId("d1").deviceId(1).modelId("m").spaceId(31L)
                    .measuredAt(at).receivedAt(at).metric(CanonicalTelemetry.Metric.of(t.metric(), v, null)).rawMessageId(1).build());
            Duration step = Duration.ofMinutes(5);
            if (t.key().equals("no-data-30m") && random.nextDouble() < 0.02) {
                step = Duration.ofMinutes(40 + random.nextInt(50));
            }
            at = at.plus(step);
        }
        return out;
    }

    /** 알람 신호 하나(비교 단위) */
    record Signal(String kind, Instant at) {
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("templates")
    @DisplayName("[RUL-01.01] TC-RUL-003 규칙 템플릿 7종 × 7일 합성 시계열 → 참조 평가기와 컴파일된 플로우의 알람 신호(RAISE·CLEAR 시각) 순서가 같다")
    void equivalent(Template template) {
        List<CanonicalTelemetry> data = series(template);
        var definition = new RuleFlowCompiler().compile(9, Jsons.MAPPER.readTree(template.rule())).definition();
        FlowTestHarness h = FlowTestHarness.of(definition, SpaceDirectory.NONE, 1);
        h.clock.set(START);
        for (CanonicalTelemetry t : data) {
            Instant due;
            while ((due = h.store.nextDue()) != null && !due.isAfter(t.measuredAt())) {
                h.advance(Duration.between(h.clock.instant(), due));
            }
            h.clock.set(t.measuredAt());
            h.send(t);
        }
        Instant due;
        while ((due = h.store.nextDue()) != null && due.isBefore(START.plus(Duration.ofDays(8)))) {
            h.advance(Duration.between(h.clock.instant(), due));
        }
        List<Signal> flow = new ArrayList<>();
        for (InMemoryExecutionStore.Outbox o : h.store.outbox()) {
            var event = CODEC.readEvent(o.action().payload().toString().getBytes(), AlarmSignal.class);
            flow.add(new Signal(event.payload().signal().name(), event.occurredAt()));
        }

        List<Signal> reference = new Reference(Jsons.MAPPER.readTree(template.rule())).run(data);

        assertThat(reference).as(template.key() + " 시계열에서 알람이 생겨야 의미가 있다").isNotEmpty();
        assertThat(flow).as(template.key()).containsExactlyElementsOf(reference);
    }

    /**
     * 참조 평가기: 규칙 의미를 플로우 없이 직접 계산한다(BR-RUL-03 지속 시간은 측정 시각 + 대기 중 만기, 한 번이라도 깨지면 처음부터; BR-RUL-04
     * 해제 기준; BR-RUL-05 연속 횟수; 무수신; 시간 조건은 발생 순간에만 적용).
     */
    static final class Reference {
        private final String kind;
        private final String op;
        private final double value;
        private final double clear;
        private final Duration forDuration;
        private final int repeat;
        private final Duration window;
        private final LocalTime from;
        private final LocalTime to;

        Reference(tools.jackson.databind.JsonNode rule) {
            var c = rule.path("condition");
            kind = c.path("kind").asString();
            op = c.path("op").asString(">");
            value = c.path("value").asDouble();
            clear = c.has("clear") ? c.path("clear").asDouble() : value;
            forDuration = Duration.parse(c.path("for").asString("PT0S"));
            repeat = c.path("repeat").asInt(1);
            window = Duration.parse(c.path("window").asString("PT30M"));
            var time = rule.path("timeCondition");
            from = time.isMissingNode() ? null : LocalTime.parse(time.path("from").asString());
            to = time.isMissingNode() ? null : LocalTime.parse(time.path("to").asString());
        }

        boolean test(double v) {
            return switch (op) {
                case ">" -> v > value;
                case "<" -> v < value;
                default -> v == value;
            };
        }

        boolean cleared(double v) {
            return switch (op) {
                case ">" -> v <= clear;
                case "<" -> v >= clear;
                default -> !test(v);
            };
        }

        boolean timeOk(Instant at) {
            if (from == null) {
                return true;
            }
            LocalTime t = at.atZone(SEOUL).toLocalTime();
            return !from.isBefore(to) ? !t.isBefore(from) || t.isBefore(to) : !t.isBefore(from) && t.isBefore(to);
        }

        List<Signal> run(List<CanonicalTelemetry> data) {
            return kind.equals("noData") ? noData(data) : threshold(data);
        }

        private List<Signal> noData(List<CanonicalTelemetry> data) {
            List<Signal> out = new ArrayList<>();
            Instant last = null;
            boolean down = false;
            for (CanonicalTelemetry t : data) {
                if (last != null && !last.plus(window).isAfter(t.measuredAt()) && !down) {
                    out.add(new Signal("RAISE", last.plus(window)));
                    down = true;
                }
                if (down) {
                    out.add(new Signal("CLEAR", t.measuredAt()));
                    down = false;
                }
                last = t.measuredAt();
            }
            // 데이터가 끝난 뒤(8일째까지)에도 무수신이면 발생
            if (last != null && !down && last.plus(window).isBefore(START.plus(Duration.ofDays(8)))) {
                out.add(new Signal("RAISE", last.plus(window)));
            }
            return out;
        }

        private List<Signal> threshold(List<CanonicalTelemetry> data) {
            List<Signal> out = new ArrayList<>();
            String phase = "IDLE";
            Instant since = null;
            Instant timerDue = null;
            boolean elapsed = false;
            int count = 0;
            for (CanonicalTelemetry t : data) {
                Instant at = t.measuredAt();
                if (phase.equals("PENDING") && timerDue != null && !timerDue.isAfter(at)) {
                    // 메시지가 오기 전에 대기 시간이 끝남(지속 타이머)
                    if (count >= repeat) {
                        phase = "ACTIVE";
                        if (timeOk(timerDue)) {
                            out.add(new Signal("RAISE", timerDue));
                        }
                    } else {
                        elapsed = true;
                    }
                    timerDue = null;
                }
                double v = t.metrics().getFirst().value();
                boolean cond = test(v);
                switch (phase) {
                    case "IDLE" -> {
                        if (!cond) {
                            continue;
                        }
                        if (forDuration.isZero() && repeat <= 1) {
                            phase = "ACTIVE";
                            if (timeOk(at)) {
                                out.add(new Signal("RAISE", at));
                            }
                        } else {
                            phase = "PENDING";
                            since = at;
                            count = 1;
                            elapsed = false;
                            timerDue = forDuration.isZero() ? null : at.plus(forDuration);
                        }
                    }
                    case "PENDING" -> {
                        if (!cond) {
                            phase = "IDLE";
                            timerDue = null;
                            continue;
                        }
                        count++;
                        boolean e = forDuration.isZero() || elapsed || !at.isBefore(since.plus(forDuration));
                        if (e && count >= repeat) {
                            phase = "ACTIVE";
                            timerDue = null;
                            if (timeOk(at)) {
                                out.add(new Signal("RAISE", at));
                            }
                        }
                    }
                    default -> {
                        if (cleared(v)) {
                            phase = "IDLE";
                            out.add(new Signal("CLEAR", at));
                        }
                    }
                }
            }
            return out;
        }
    }
}
