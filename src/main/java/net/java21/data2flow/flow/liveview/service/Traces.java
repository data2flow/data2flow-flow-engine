package net.java21.data2flow.flow.liveview.service;

import net.java21.data2flow.contracts.command.ActionKind;
import net.java21.data2flow.contracts.flow.FlowTrace;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 실행 보고서 → 실행 추적(API-FLW-41 Trace, contracts {@link FlowTrace}). 시험 실행(API-FLW-12) 응답도 같은 모양이다. 노드 순서·입출력·
 * 소요 시간(ms)·나간 포트·행동(종류·멱등 키·드라이런·건너뜀)을 담고, 분기에서 선택되지 않은 경로는 실행하지 않았으므로 들어가지 않는다.
 */
public final class Traces {

    private Traces() {
    }

    public static FlowTrace of(ExecutionReport report) {
        return merge(List.of(report), report.startedAt());
    }

    /**
     * 여러 실행(시험 실행의 트리거 실행과 앞당겨 발화한 타이머 실행)을 추적 하나로 잇는다. 뒤 실행 단계의 {@code inMs}는 첫 실행 시작부터 잰다.
     */
    public static FlowTrace merge(List<ExecutionReport> reports, Instant start) {
        List<FlowTrace.Step> steps = new ArrayList<>();
        ObjectNode error = null;
        String result = FlowTrace.COMPLETED;
        for (ExecutionReport report : reports) {
            double offsetMs = Duration.between(start, report.startedAt()).toNanos() / 1_000_000.0;
            for (ExecutionReport.Step s : report.steps()) {
                List<FlowTrace.Output> outputs = new ArrayList<>();
                for (ExecutionReport.Output o : s.outputs()) {
                    outputs.add(new FlowTrace.Output(o.port(), o.payload()));
                }
                if (outputs.isEmpty()) {
                    for (String port : s.ports()) {
                        outputs.add(new FlowTrace.Output(port, null));
                    }
                }
                FlowTrace.ActionRecord action = null;
                if (!s.actions().isEmpty()) {
                    ExecutionReport.ActionRecord a = s.actions().getFirst();
                    action = new FlowTrace.ActionRecord(kind(a.kind()), a.idempotencyKey(), a.dryRun(), a.skipped());
                }
                steps.add(new FlowTrace.Step(s.nodeId(), s.type() == null ? "unknown" : s.type(),
                        offsetMs + s.inMicros() / 1000.0, s.durationMicros() / 1000.0, s.input(), outputs, action));
                if (s.errorType() != null && s.ports().contains("error") && error == null) {
                    error = Jsons.object().put("nodeId", s.nodeId()).put("errorType", s.errorType())
                            .put("message", s.errorMessage());
                    result = "FLOW_HOP_LIMIT".equals(s.errorType()) ? FlowTrace.HOP_LIMIT : FlowTrace.FAILED;
                }
            }
        }
        ExecutionReport first = reports.getFirst();
        return new FlowTrace(first.triggerMessageId(), first.flowId().toString(), first.version(), start, steps, result, error);
    }

    private static ActionKind kind(String kind) {
        try {
            return ActionKind.valueOf(kind);
        } catch (IllegalArgumentException | NullPointerException e) {
            return ActionKind.UNKNOWN;
        }
    }
}
