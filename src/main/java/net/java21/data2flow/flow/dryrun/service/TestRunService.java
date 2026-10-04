package net.java21.data2flow.flow.dryrun.service;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.flow.FlowTrace;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.common.FlowEngineErrorCode;
import net.java21.data2flow.flow.dryrun.dto.TestRunRequest;
import net.java21.data2flow.flow.liveview.service.Traces;
import net.java21.data2flow.flow.node.service.TelemetryTriggerNodeType;
import net.java21.data2flow.flow.plan.domain.ExecutionPlan;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.domain.Overlay;
import net.java21.data2flow.flow.plan.domain.PlanNode;
import net.java21.data2flow.flow.plan.domain.TimerFire;
import net.java21.data2flow.flow.plan.domain.TriggerNode;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.domain.ActionGuard;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.runtime.domain.InMemoryExecutionStore;
import net.java21.data2flow.flow.runtime.service.FlowExecutor;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 시험 실행(FLW-03.05, API-FLW-12 내부 {@code POST /internal/flow/test-runs}, BR-FLW-11): 메시지 하나를 플로우에 넣어 끝까지 드라이런한다.
 * 행동(제어·알림·Sink·알람)은 기록만 하고(추적 {@code action.dryRun=true}), 노드 상태·타이머는 메모리 저장소를 써서 운영 상태
 * ({@code flow_node_state})가 바뀌지 않는다. 대기·지속 타이머는 가상 시각으로 앞당겨 발화해 "끝까지"(최대 24시간·타이머 100개) 실행한다.
 *
 * <ul>
 *   <li>입력: {@code input.message}(표준 텔레메트리) 또는 {@code input.body}(플로우 메시지). {@code rawMessageId}는 원본을 읽을 수 있는
 *       core-api가 텔레메트리로 바꿔 {@code input.message}로 넘긴다(엔진은 다른 서비스 스키마를 읽지 않음).</li>
 *   <li>시작: {@code startNodeId}가 있으면 그 노드부터, 없으면 맞는 트리거(맞는 트리거가 없으면 첫 트리거)부터.</li>
 *   <li>정의: {@code definition}(편집 중인 초안). 없으면 적재된 실행 계획.</li>
 * </ul>
 */
public class TestRunService {

    static final int MAX_TIMER_FIRES = 100;
    static final Duration MAX_VIRTUAL_TIME = Duration.ofHours(24);

    private final FlowCompiler compiler;
    private final FlowRegistry registry;
    private final FlowExecutor executor;
    private final ActionGuard guard;
    private final Clock clock;

    public TestRunService(FlowCompiler compiler, FlowRegistry registry, FlowExecutor executor, ActionGuard guard, Clock clock) {
        this.compiler = compiler;
        this.registry = registry;
        this.executor = executor;
        this.guard = guard == null ? ActionGuard.NONE : guard;
        this.clock = clock;
    }

    public FlowTrace run(TestRunRequest request) {
        UUID flowId = parseFlowId(request.flowId());
        JsonNode input = request.input();
        if (input == null || !input.isObject()) {
            throw invalid("input이 필요합니다({message} 또는 {body})");
        }
        CanonicalTelemetry telemetry = null;
        if (input.hasNonNull("message")) {
            try {
                telemetry = Jsons.MAPPER.treeToValue(input.get("message"), CanonicalTelemetry.class);
            } catch (RuntimeException e) {
                throw invalid("input.message가 표준 텔레메트리 형식이 아닙니다: " + e.getMessage());
            }
        } else if (!input.hasNonNull("body")) {
            throw invalid(input.has("rawMessageId")
                    ? "rawMessageId는 core-api가 원본을 읽어 input.message로 넘겨야 합니다" : "input.message 또는 input.body가 필요합니다");
        }
        long organizationId = organization(request, telemetry, flowId);
        ExecutionPlan plan = plan(flowId, organizationId, request);
        Instant start = clock.instant();
        FlowExecutor.RunOptions options = new FlowExecutor.RunOptions(true, start, guard);
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        List<ExecutionReport> reports = new ArrayList<>();
        for (Start s : starts(plan, request.startNodeId(), telemetry, input.get("body"))) {
            reports.add(executor.runTrigger(plan, Overlay.NONE, s.node(), s.message(), store, options));
        }
        if (reports.isEmpty()) {
            throw invalid("시작할 노드가 없습니다");
        }
        reports.addAll(fireUntil(executor, plan, store, start.plus(MAX_VIRTUAL_TIME), options, MAX_TIMER_FIRES));
        return Traces.merge(reports, start);
    }

    /**
     * 메모리 저장소의 대기 타이머를 가상 시각으로 앞당겨 발화한다(시험 실행·과거 재생 공용, BR-FLW-11).
     *
     * @param until    이 시각까지 만기가 된 타이머만
     * @param maxFires 최대 발화 수
     */
    public static List<ExecutionReport> fireUntil(FlowExecutor executor, ExecutionPlan plan, InMemoryExecutionStore store,
                                                  Instant until, FlowExecutor.RunOptions options, int maxFires) {
        List<ExecutionReport> out = new ArrayList<>();
        while (out.size() < maxFires) {
            Instant due = store.nextDue();
            if (due == null || due.isAfter(until)) {
                break;
            }
            for (InMemoryExecutionStore.Timer t : store.takeDue(due)) {
                PlanNode node = plan.node(t.nodeId());
                if (node != null) {
                    out.add(executor.runTimer(plan, Overlay.NONE, node, fireOf(t), store, options.at(due)));
                }
            }
        }
        return out;
    }

    private record Start(PlanNode node, FlowMessage message) {
    }

    private List<Start> starts(ExecutionPlan plan, String startNodeId, CanonicalTelemetry telemetry, JsonNode body) {
        FlowMessage message;
        if (telemetry != null) {
            message = TelemetryTriggerNodeType.toMessage(telemetry);
        } else {
            if (!body.isObject()) {
                throw invalid("input.body는 JSON 객체여야 합니다");
            }
            ObjectNode b = (ObjectNode) body.deepCopy();
            String id = Jsons.text(b, "messageId") == null ? UUID.randomUUID().toString() : Jsons.text(b, "messageId");
            b.put("messageId", id);
            String device = Jsons.text(b, "deviceId");
            message = new FlowMessage(b, id, device == null ? "test" : "device:" + device);
        }
        message = message.withRunKey(plan.mode().runKey(message));
        if (startNodeId != null && !startNodeId.isBlank()) {
            PlanNode node = plan.node(startNodeId);
            if (node == null) {
                throw invalid("startNodeId 노드가 없습니다: " + startNodeId);
            }
            return List.of(new Start(node, message));
        }
        List<Start> out = new ArrayList<>();
        if (telemetry != null) {
            for (PlanNode trigger : plan.triggers()) {
                Optional<FlowMessage> m = ((TriggerNode) trigger.compiled()).match(telemetry);
                FlowMessage start = message;
                m.ifPresent(x -> out.add(new Start(trigger, start)));
            }
        }
        if (out.isEmpty() && !plan.triggers().isEmpty()) {
            out.add(new Start(plan.triggers().getFirst(), message));   // 대상이 달라도 첫 트리거부터 보여 준다
        }
        return out;
    }

    private long organization(TestRunRequest request, CanonicalTelemetry telemetry, UUID flowId) {
        if (request.organizationId() != null && !request.organizationId().isBlank()) {
            try {
                return Long.parseLong(request.organizationId());
            } catch (NumberFormatException e) {
                throw invalid("organizationId가 숫자가 아닙니다");
            }
        }
        if (telemetry != null) {
            return telemetry.organizationId();
        }
        return registry.get(flowId).map(LoadedFlow::organizationId).orElse(1L);
    }

    private ExecutionPlan plan(UUID flowId, long organizationId, TestRunRequest request) {
        FlowDefinition definition = request.definition();
        if (definition == null) {
            return registry.get(flowId).filter(f -> request.version() == null || f.version() == request.version())
                    .map(LoadedFlow::plan).orElseThrow(() -> invalid("definition이 필요합니다(적재된 같은 버전이 없음)"));
        }
        FlowCompiler.Result result = compiler.compile(flowId, organizationId, request.version() == null ? 0 : request.version(),
                definition);
        if (!result.ok()) {
            throw new BusinessException(FlowEngineErrorCode.FLOW_VALIDATION_FAILED, result.errors().stream()
                    .map(e -> new FieldErrorDetail(e.field(), e.code(), e.message())).toList());
        }
        return result.plan();
    }

    static UUID parseFlowId(String flowId) {
        if (flowId == null || flowId.isBlank()) {
            return new UUID(0, 0);
        }
        try {
            return UUID.fromString(flowId);
        } catch (IllegalArgumentException e) {
            throw invalid("flowId가 UUID가 아닙니다");
        }
    }

    static BusinessException invalid(String reason) {
        return new BusinessException(FlowEngineErrorCode.FLOW_TEST_INPUT_INVALID, List.of(new FieldErrorDetail("input",
                "INVALID", reason)));
    }

    /** 시험 실행의 타이머 앞당기기에 쓰는 발화 하나 */
    static TimerFire fireOf(InMemoryExecutionStore.Timer t) {
        return new TimerFire(t.id(), t.kind(), t.targetKey(), t.dueAt(), t.context(), t.flowVersion());
    }
}
