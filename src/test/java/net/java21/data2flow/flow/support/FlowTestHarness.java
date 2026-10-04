package net.java21.data2flow.flow.support;

import net.java21.data2flow.contracts.capability.CapabilityCatalog;
import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.node.service.AggregateTransformNodeType;
import net.java21.data2flow.flow.node.service.AlarmActionNodeType;
import net.java21.data2flow.flow.node.service.ControlActionNodeType;
import net.java21.data2flow.flow.node.service.DebugLogNodeType;
import net.java21.data2flow.flow.node.service.DelayFlowNodeType;
import net.java21.data2flow.flow.node.service.GroupConditionNodeType;
import net.java21.data2flow.flow.node.service.JsFunctionNodeType;
import net.java21.data2flow.flow.node.service.MapTransformNodeType;
import net.java21.data2flow.flow.node.service.NoDataConditionNodeType;
import net.java21.data2flow.flow.node.service.NotifyActionNodeType;
import net.java21.data2flow.flow.node.service.RateOfChangeConditionNodeType;
import net.java21.data2flow.flow.node.service.ScriptDirectory;
import net.java21.data2flow.flow.node.service.SinkDatabaseNodeType;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.node.service.SwitchConditionNodeType;
import net.java21.data2flow.flow.node.service.TelemetryTriggerNodeType;
import net.java21.data2flow.flow.node.service.ThresholdConditionNodeType;
import net.java21.data2flow.flow.node.service.TimeWindowConditionNodeType;
import net.java21.data2flow.flow.plan.domain.ExecutionPlan;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.Overlay;
import net.java21.data2flow.flow.plan.domain.PlanNode;
import net.java21.data2flow.flow.plan.domain.TimerFire;
import net.java21.data2flow.flow.plan.domain.TriggerNode;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.plan.service.NodeTypeRegistry;
import net.java21.data2flow.flow.runtime.domain.ActionGuard;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.runtime.domain.InMemoryExecutionStore;
import net.java21.data2flow.flow.runtime.service.FlowExecutor;
import net.java21.data2flow.script.sandbox.ScriptSandbox;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 노드·플로우 단위 시험 하네스(FLW test-plan "FlowTestHarness"): 운영과 같은 노드 레지스트리·컴파일러·실행기를 메모리 저장소와
 * {@link MutableClock}으로 돌린다. 실제 대기 없이 {@link #advance}로 시간을 돌려 지속 타이머를 발화한다. {@link #apply}는 같은 저장소·시계로
 * 새 버전을 적용한 하네스를 돌려준다(라이브 리로드 상태 이어받기 시험).
 */
public final class FlowTestHarness {

    public static final ScriptSandbox SANDBOX = SandboxHarness.sandbox();
    public static final FlowEngineProperties.Execution LIMITS = new FlowEngineProperties.Execution(Duration.ofMillis(10),
            Duration.ofMillis(100), 100, 100, 262_144, Duration.ofSeconds(600), 0.1);

    public final MutableClock clock;
    public final InMemoryExecutionStore store;
    public final List<ExecutionReport> reports = new ArrayList<>();
    private final ExecutionPlan plan;
    private final NodeTypeRegistry registry;
    private final FlowExecutor executor;
    private Overlay overlay = Overlay.NONE;
    private ActionGuard guard = ActionGuard.NONE;
    private boolean dryRun;

    private FlowTestHarness(ExecutionPlan plan, NodeTypeRegistry registry, InMemoryExecutionStore store, MutableClock clock) {
        this.plan = plan;
        this.registry = registry;
        this.store = store;
        this.clock = clock;
        this.executor = new FlowExecutor(clock, LIMITS);
    }

    public static NodeTypeRegistry registry(SpaceDirectory spaces) {
        return registry(spaces, ScriptDirectory.NONE);
    }

    public static NodeTypeRegistry registry(SpaceDirectory spaces, ScriptDirectory scripts) {
        return new NodeTypeRegistry(List.of(
                new TelemetryTriggerNodeType(spaces), new ThresholdConditionNodeType(), new SwitchConditionNodeType(),
                new MapTransformNodeType(), new AggregateTransformNodeType(), new JsFunctionNodeType(SANDBOX, scripts),
                new DelayFlowNodeType(), new ControlActionNodeType(CapabilityCatalog.standard(), Duration.ofSeconds(600)),
                new DebugLogNodeType(), new SinkDatabaseNodeType(), new NotifyActionNodeType(), new AlarmActionNodeType(),
                new NoDataConditionNodeType(), new RateOfChangeConditionNodeType(), new TimeWindowConditionNodeType(),
                new GroupConditionNodeType()));
    }

    public static FlowTestHarness of(FlowDefinition definition) {
        return of(definition, SpaceDirectory.NONE, 1);
    }

    public static FlowTestHarness of(FlowDefinition definition, SpaceDirectory spaces, int version) {
        return of(definition, registry(spaces), version);
    }

    public static FlowTestHarness of(FlowDefinition definition, NodeTypeRegistry registry, int version) {
        return new FlowTestHarness(compile(registry, definition, version), registry, new InMemoryExecutionStore(),
                MutableClock.atUtc("2026-03-02T00:00:00Z"));
    }

    private static ExecutionPlan compile(NodeTypeRegistry registry, FlowDefinition definition, int version) {
        FlowCompiler.Result r = new FlowCompiler(registry).compile(FlowFixtures.FLOW, FlowFixtures.ORG, version, definition);
        if (!r.ok()) {
            throw new IllegalArgumentException("컴파일 실패: " + r.errors());
        }
        return r.plan();
    }

    /** 같은 저장소·시계로 새 버전을 적용한 하네스(라이브 리로드: 상태는 저장소에 있고 계획만 바뀐다) */
    public FlowTestHarness apply(FlowDefinition next, int version) {
        FlowTestHarness h = new FlowTestHarness(compile(registry, next, version), registry, store, clock);
        h.overlay = overlay;
        h.guard = guard;
        h.dryRun = dryRun;
        return h;
    }

    /**
     * 노드 하나를 시험한다: 기기 1~1000 텔레메트리 트리거 → 시험할 노드({@code n-test0001}). 출력은 보고서 단계에서 본다.
     */
    public static FlowTestHarness node(String type, String configJson) {
        return of(FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1",
                 "nodes":[{"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"em300-th"}}},
                          {"id":"n-test0001","type":"%s","config":%s}],
                 "wires":[{"from":"n-trg00001","to":"n-test0001"}]}""".formatted(type, configJson)));
    }

    public ExecutionPlan plan() {
        return plan;
    }

    public FlowTestHarness overlay(Overlay value) {
        this.overlay = value;
        return this;
    }

    public FlowTestHarness guard(ActionGuard value) {
        this.guard = value;
        return this;
    }

    public FlowTestHarness dryRun(boolean value) {
        this.dryRun = value;
        return this;
    }

    private FlowExecutor.RunOptions options() {
        return new FlowExecutor.RunOptions(dryRun, null, guard);
    }

    /** 텔레메트리를 넣는다(맞는 트리거마다 실행) */
    public List<ExecutionReport> send(CanonicalTelemetry telemetry) {
        List<ExecutionReport> out = new ArrayList<>();
        for (PlanNode trigger : plan.triggers()) {
            Optional<FlowMessage> m = ((TriggerNode) trigger.compiled()).match(telemetry);
            m.ifPresent(message -> out.add(executor.runTrigger(plan, overlay, trigger,
                    message.withRunKey(plan.mode().runKey(message)), store, options())));
        }
        reports.addAll(out);
        return out;
    }

    /** 노드에 메시지를 바로 넣는다(본문 JSON, 대상 키 device:1) */
    public ExecutionReport sendTo(String nodeId, String bodyJson) {
        ObjectNode body = (ObjectNode) Jsons.MAPPER.readTree(bodyJson);
        String id = body.has("messageId") ? body.get("messageId").asString() : UUID.randomUUID().toString();
        String key = body.has("deviceId") ? "device:" + body.get("deviceId").asString() : "device:1";
        ExecutionReport r = executor.runTrigger(plan, overlay, plan.node(nodeId), new FlowMessage(body, id, key), store, options());
        reports.add(r);
        return r;
    }

    /** 시간을 돌리고 만기가 된 타이머를 발화한다 */
    public List<ExecutionReport> advance(Duration duration) {
        clock.advance(duration);
        List<ExecutionReport> out = new ArrayList<>();
        for (InMemoryExecutionStore.Timer t : store.takeDue(clock.instant())) {
            PlanNode node = plan.node(t.nodeId());
            if (node != null) {
                out.add(executor.runTimer(plan, overlay, node,
                        new TimerFire(t.id(), t.kind(), t.targetKey(), t.dueAt(), t.context(), t.flowVersion()), store, options()));
            }
        }
        reports.addAll(out);
        return out;
    }

    /** 대기 중인 타이머 하나를 결과와 함께 바로 발화한다(제어 결과 EVT-ACT-01 이어 붙이기) */
    public ExecutionReport resume(long timerId, JsonNode result) {
        InMemoryExecutionStore.Timer t = store.take(timerId);
        if (t == null) {
            throw new IllegalArgumentException("대기 타이머가 없습니다: " + timerId);
        }
        ObjectNode context = (ObjectNode) t.context().deepCopy();
        context.set("result", result);
        ExecutionReport r = executor.runTimer(plan, overlay, plan.node(t.nodeId()),
                new TimerFire(t.id(), t.kind(), t.targetKey(), t.dueAt(), context, t.flowVersion()), store, options());
        reports.add(r);
        return r;
    }

    /** 보고서들에서 노드가 내보낸 (포트, 본문) */
    public static List<ExecutionReport.Output> outputs(List<ExecutionReport> reports, String nodeId) {
        return reports.stream().flatMap(r -> r.steps().stream()).filter(s -> s.nodeId().equals(nodeId))
                .flatMap(s -> s.outputs().stream()).toList();
    }

    public static List<String> ports(List<ExecutionReport> reports, String nodeId) {
        return reports.stream().flatMap(r -> r.steps().stream()).filter(s -> s.nodeId().equals(nodeId))
                .flatMap(s -> s.ports().stream()).toList();
    }

    public static List<ExecutionReport.Step> errors(List<ExecutionReport> reports, String nodeId) {
        return reports.stream().flatMap(r -> r.steps().stream())
                .filter(s -> s.nodeId().equals(nodeId) && s.errorType() != null).toList();
    }

    /** 보고서들의 행동 기록 */
    public static List<ExecutionReport.ActionRecord> actions(List<ExecutionReport> reports) {
        return reports.stream().flatMap(r -> r.steps().stream()).flatMap(s -> s.actions().stream()).toList();
    }

    public JsonNode state(String nodeId, String targetKey) {
        return store.state(plan.flowId(), nodeId, targetKey);
    }
}
