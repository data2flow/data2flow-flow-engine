package net.java21.data2flow.flow.runtime.service;

import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.contracts.flow.StatePolicy;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.plan.domain.ActionDraft;
import net.java21.data2flow.flow.plan.domain.DebugSample;
import net.java21.data2flow.flow.plan.domain.ExecutionPlan;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.NodeContext;
import net.java21.data2flow.flow.plan.domain.Overlay;
import net.java21.data2flow.flow.plan.domain.PlanNode;
import net.java21.data2flow.flow.plan.domain.TimerFire;
import net.java21.data2flow.flow.plan.domain.TimerKind;
import net.java21.data2flow.flow.runtime.domain.ActionGuard;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.runtime.domain.ExecutionStore;
import net.java21.data2flow.flow.runtime.domain.NodeMetrics;
import net.java21.data2flow.flow.runtime.domain.StoredState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;

/**
 * 실행 계획 하나로 메시지 하나(또는 타이머 발화 하나)를 끝까지 처리한다(FLW-05.01·05.02·05.03·08.01·08.02·10.02·10.03).
 *
 * <ul>
 *   <li><b>버전 고정</b>: 호출하는 쪽이 넘긴 계획 하나만 쓴다(BR-FLW-06). 보고서에 그 버전을 남긴다.</li>
 *   <li><b>실행 순서</b>: 위상 순서(작은 것부터). 노드 상태 잠금도 이 순서로 걸려 같은 계획을 쓰는 실행끼리 교착이 없다.</li>
 *   <li><b>상태 이어받기</b>(FLW-06.03, BR-FLW-07): 상태 행의 지문(쓴 노드의 설정)이 지금 노드와 다르면 노드 종류의 상태 정책을 적용해 읽는다
 *       (KEEP 그대로, RESET 빈 상태, MIGRATE 변환). 쓸 때는 지금 노드의 지문을 함께 쓴다.</li>
 *   <li><b>재시도</b>(FLW-08.01, BR-FLW-21): 노드가 실패하고 재시도가 남았으면 RETRY 타이머(백오프)로 미뤄 같은 노드를 다시 실행한다. 같은 대상
 *       키의 다음 메시지를 막지 않는다. 한도·타입 오류처럼 다시 해도 같은 오류는 재시도하지 않는다.</li>
 *   <li><b>오류 격리</b>(FLW-05.03·08.02): 최종 실패는 그 노드의 error 포트로 {@code {…원래 메시지, error:{nodeId, errorType, message,
 *       attempts}}}가 나가고, error 포트에 와이어가 없으면 그 갈래만 끝나고 오류 지표가 1 오른다. 저장소(DB) 장애만 밖으로 던져 트랜잭션을
 *       되돌리고 다시 시도하게 한다(유실 0).</li>
 *   <li><b>한도</b>(BR-FLW-16): 메시지 하나가 거치는 노드 수 100({@code FLOW_HOP_LIMIT}), 노드 하나가 입력 하나에 내보내는 메시지 100
 *       ({@code FLOW_FANOUT_LIMIT}: 그 실행을 멈추고 error 포트로), 노드 상태 256KB, 플로우당 대기 타이머 10,000({@code FLOW_TIMER_LIMIT}).</li>
 *   <li><b>바이패스</b>(BR-FLW-10): 변환·조건 노드는 입력을 첫 출력 포트로 넘기고, 행동 노드는 실행하지 않고 "bypassed"를 기록한다.</li>
 *   <li><b>행동 확인</b>(BR-FLW-19): 기기 제어·장면 행동은 {@link ActionGuard}(비상 정지·유지보수)가 막으면 내보내지 않고 "skipped(사유)"를 기록한다.</li>
 * </ul>
 */
public class FlowExecutor {

    private static final Logger log = LoggerFactory.getLogger(FlowExecutor.class);
    /** 다시 해도 같은 결과인 오류(재시도하지 않음) */
    static final Set<String> NOT_RETRYABLE = Set.of("TYPE_MISMATCH", "NODE_STATE_TOO_LARGE", "FLOW_HOP_LIMIT",
            "FLOW_FANOUT_LIMIT", "FLOW_TIMER_LIMIT", "INVALID_MESSAGE");
    /** 플로우당 대기 타이머 상한(BR-FLW-16) */
    public static final long MAX_WAITING_TIMERS = 10_000;
    /** 상태 지문·타이머 문맥의 실행 정보 키 */
    public static final String RUN_KEY = "@run";

    private final Clock clock;
    private final FlowEngineProperties.Execution limits;

    public FlowExecutor(Clock clock, FlowEngineProperties.Execution limits) {
        this.clock = clock;
        this.limits = limits;
    }

    /**
     * 실행 옵션.
     *
     * @param dryRun 드라이런(BR-FLW-11): 행동은 기록만, 지표는 쓰지 않음
     * @param now    처리 시각(재생은 재생 중인 시각). null이면 시계
     * @param guard  행동 확인(비상 정지·유지보수)
     */
    public record RunOptions(boolean dryRun, Instant now, ActionGuard guard) {

        public static final RunOptions LIVE = new RunOptions(false, null, ActionGuard.NONE);
        public static final RunOptions DRY_RUN = new RunOptions(true, null, ActionGuard.NONE);

        public RunOptions {
            guard = guard == null ? ActionGuard.NONE : guard;
        }

        public static RunOptions of(boolean dryRun) {
            return dryRun ? DRY_RUN : LIVE;
        }

        public RunOptions at(Instant instant) {
            return new RunOptions(dryRun, instant, guard);
        }
    }

    /** 트리거가 받은 메시지를 처리한다 */
    public ExecutionReport runTrigger(ExecutionPlan plan, Overlay overlay, PlanNode trigger, FlowMessage message,
                                      ExecutionStore store, boolean dryRun) {
        return runTrigger(plan, overlay, trigger, message, store, RunOptions.of(dryRun));
    }

    public ExecutionReport runTrigger(ExecutionPlan plan, Overlay overlay, PlanNode trigger, FlowMessage message,
                                      ExecutionStore store, RunOptions options) {
        Run run = new Run(plan, overlay, store, options, message.triggerMessageId(), message.runKey());
        run.enqueue(trigger, message, 0, 1);
        run.drain();
        return run.finish();
    }

    /** 만기가 된 타이머를 그 노드의 {@code onTimer}로 넘기고(재시도 타이머는 노드를 다시 실행하고) 이어서 처리한다 */
    public ExecutionReport runTimer(ExecutionPlan plan, Overlay overlay, PlanNode node, TimerFire fire, ExecutionStore store,
                                    boolean dryRun) {
        return runTimer(plan, overlay, node, fire, store, RunOptions.of(dryRun));
    }

    public ExecutionReport runTimer(ExecutionPlan plan, Overlay overlay, PlanNode node, TimerFire fire, ExecutionStore store,
                                    RunOptions options) {
        JsonNode context = fire.context() == null ? Jsons.object() : fire.context();
        JsonNode runInfo = context.path(RUN_KEY);
        String trigger = runInfo.path("id").asString(null);
        if (trigger == null) {
            trigger = context.path("triggerMessageId").asString("timer:" + fire.timerId());
        }
        Run run = new Run(plan, overlay, store, options, trigger, runInfo.path("key").asString(null));
        run.fire(node, fire);
        run.drain();
        return run.finish();
    }

    /** 노드 상태가 한도를 넘었다(BR-FLW-29) */
    static final class StateTooLargeException extends RuntimeException {
        StateTooLargeException(String message) {
            super(message, null, false, false);
        }
    }

    /** 플로우 대기 타이머가 한도다(BR-FLW-16) */
    static final class TimerLimitException extends RuntimeException {
        TimerLimitException(String message) {
            super(message, null, false, false);
        }
    }

    private record Item(PlanNode node, FlowMessage message, int hops, long sequence, int attempt) {
    }

    /** 행동 노드의 추적 종류(바이패스로 실행하지 않아 행동 요청이 없을 때) */
    static String actionKindOf(PlanNode node) {
        String type = node.definition().type();
        if (type.startsWith("sink.")) {
            return "SINK";
        }
        return switch (type) {
            case "action.notify" -> "NOTIFY";
            case "action.scene" -> "SCENE";
            case "action.alarm" -> "EVENT";
            default -> "COMMAND";
        };
    }

    private final class Run {
        private final ExecutionPlan plan;
        private final Overlay overlay;
        private final ExecutionStore store;
        private final RunOptions options;
        private final boolean dryRun;
        private final Instant now;
        private final long startedNanos = System.nanoTime();
        private final String triggerMessageId;
        private final String runKey;
        private final PriorityQueue<Item> queue = new PriorityQueue<>(
                Comparator.comparingInt((Item i) -> i.node().order()).thenComparingLong(Item::sequence));
        private final Map<String, NodeMetrics> metrics = new LinkedHashMap<>();
        private final List<ExecutionReport.Step> steps = new ArrayList<>();
        private final List<ActionDraft> actions = new ArrayList<>();
        private final List<DebugSample> samples = new ArrayList<>();
        private final Map<String, JsonNode> stateCache = new HashMap<>();
        private Long waitingTimers;
        private long sequence;
        private int errors;

        Run(ExecutionPlan plan, Overlay overlay, ExecutionStore store, RunOptions options, String triggerMessageId,
            String runKey) {
            this.plan = plan;
            this.overlay = overlay == null ? Overlay.NONE : overlay;
            this.store = store;
            this.options = options;
            this.dryRun = options.dryRun();
            this.now = options.now() == null ? clock.instant() : options.now();
            this.triggerMessageId = triggerMessageId;
            this.runKey = runKey;
        }

        void enqueue(PlanNode node, FlowMessage message, int hops, int attempt) {
            queue.add(new Item(node, message, hops, sequence++, attempt));
        }

        void drain() {
            while (!queue.isEmpty()) {
                process(queue.poll());
            }
        }

        void fire(PlanNode node, TimerFire fire) {
            JsonNode context = fire.context() == null ? Jsons.object() : fire.context();
            if (fire.kind() == TimerKind.RETRY) {
                JsonNode body = context.get("message");
                String trigger = context.path("triggerMessageId").asString(triggerMessageId);
                FlowMessage message = new FlowMessage(body instanceof ObjectNode o ? o.deepCopy() : Jsons.object(),
                        trigger == null ? "timer:" + fire.timerId() : trigger, fire.targetKey(), runKey);
                process(new Item(node, message, context.path("hops").asInt(0), sequence++, context.path("attempt").asInt(2)));
                return;
            }
            Ctx ctx = new Ctx(node, null, 0, 1);
            long started = System.nanoTime();
            try {
                node.compiled().onTimer(fire, ctx);
            } catch (DataAccessException e) {
                throw e;
            } catch (StateTooLargeException e) {
                ctx.failTimer("NODE_STATE_TOO_LARGE", e.getMessage(), fire);
            } catch (TimerLimitException e) {
                ctx.failTimer("FLOW_TIMER_LIMIT", e.getMessage(), fire);
            } catch (RuntimeException e) {
                log.warn("플로우 {} v{} 노드 {} 타이머 처리 오류: {}", plan.flowId(), plan.version(), node.id(), e.toString());
                ctx.failTimer("NODE_EXCEPTION", e.toString(), fire);
            }
            finishNode(node, ctx, started);
        }

        void process(Item item) {
            PlanNode node = item.node();
            Ctx ctx = new Ctx(node, item.message(), item.hops(), item.attempt());
            long started = System.nanoTime();
            if (item.hops() >= limits.maxHops()) {
                ctx.finalFail(item.message(), "FLOW_HOP_LIMIT", "메시지 하나가 거치는 노드 수가 " + limits.maxHops() + "를 넘었습니다");
                finishNode(node, ctx, started);
                return;
            }
            if (overlay.debug().contains(node.id())) {
                samples.add(new DebugSample(node.id(), item.message().triggerMessageId(), "in", null, item.message().body(), true));
            }
            try {
                if (overlay.bypass().contains(node.id())) {
                    if (node.compiled().isAction()) {
                        ctx.actionRecords.add(new ExecutionReport.ActionRecord(actionKindOf(node), null, dryRun, "bypassed", null));
                        ObjectNode note = Jsons.object().put("skipped", "bypassed");
                        samples.add(new DebugSample(node.id(), item.message().triggerMessageId(), "out", null, note, true));
                    } else {
                        ctx.emit(node.compiled().outputs().getFirst(), item.message());
                    }
                } else {
                    node.compiled().onMessage(item.message(), ctx);
                }
            } catch (DataAccessException e) {
                throw e;   // 저장소 장애: 트랜잭션을 되돌리고 다시 시도
            } catch (StateTooLargeException e) {
                ctx.fail(item.message(), "NODE_STATE_TOO_LARGE", e.getMessage());
            } catch (TimerLimitException e) {
                ctx.fail(item.message(), "FLOW_TIMER_LIMIT", e.getMessage());
            } catch (RuntimeException e) {
                log.warn("플로우 {} v{} 노드 {} 처리 오류: {}", plan.flowId(), plan.version(), node.id(), e.toString());
                ctx.fail(item.message(), "NODE_EXCEPTION", e.toString());
            }
            finishNode(node, ctx, started);
        }

        private void finishNode(PlanNode node, Ctx ctx, long started) {
            long end = System.nanoTime();
            if (ctx.emissions.size() > limits.maxFanout()) {
                // 무한 분기 방지(BR-FLW-16): 이 실행의 남은 갈래를 멈추고 error 포트로 보낸다
                int count = ctx.emissions.size();
                ctx.emissions.clear();
                ctx.ports.clear();
                queue.clear();
                ctx.finalFail(ctx.input == null ? new FlowMessage(Jsons.object(), triggerMessageId, "*") : ctx.input,
                        "FLOW_FANOUT_LIMIT", "노드 하나가 입력 하나에 내보내는 메시지가 " + limits.maxFanout() + "를 넘었습니다(" + count + "건)");
            }
            NodeMetrics m = metrics.computeIfAbsent(node.id(), k -> new NodeMetrics());
            m.processed((end - started) / 1000);
            steps.add(new ExecutionReport.Step(node.id(), node.definition().type(), (started - startedNanos) / 1000,
                    (end - started) / 1000, ctx.input == null ? null : ctx.input.body(), List.copyOf(ctx.ports), ctx.errorType,
                    ctx.errorMessage, ctx.emissions.stream().map(e -> new ExecutionReport.Output(e.port(), e.message().body()))
                    .toList(), List.copyOf(ctx.actionRecords)));
            if (ctx.finalError) {
                m.error();
                errors++;
            }
            for (ExecutionReport.ActionRecord a : ctx.actionRecords) {
                if (a.skipped() == null) {
                    m.action(a.kind());
                }
            }
            for (Emission e : ctx.emissions) {
                if (overlay.debug().contains(node.id())) {
                    samples.add(new DebugSample(node.id(), e.message().triggerMessageId(), "out", e.port(), e.message().body(), true));
                }
                List<String> targets = node.targets(e.port());
                for (int i = 0; i < targets.size(); i++) {
                    PlanNode next = plan.node(targets.get(i));
                    if (next != null) {
                        enqueue(next, i == 0 ? e.message() : e.message().copy(), ctx.hops + 1, 1);
                    }
                }
            }
        }

        ExecutionReport finish() {
            long micros = (System.nanoTime() - startedNanos) / 1000;
            if (!dryRun) {
                Instant minute = now.truncatedTo(ChronoUnit.MINUTES);
                NodeMetrics flow = new NodeMetrics();
                flow.execution(micros);
                if (errors > 0) {
                    flow.error();
                }
                for (ActionDraft a : actions) {
                    flow.action(a.kind());
                }
                metrics.forEach((nodeId, m) -> store.addMetrics(plan.flowId(), plan.organizationId(), nodeId, minute, m));
                store.addMetrics(plan.flowId(), plan.organizationId(), NodeMetrics.FLOW_NODE, minute, flow);
            }
            return new ExecutionReport(plan.flowId(), plan.version(), triggerMessageId, now, micros, List.copyOf(steps),
                    List.copyOf(actions), List.copyOf(samples), errors, Map.copyOf(metrics));
        }

        private record Emission(String port, FlowMessage message) {
        }

        /** 노드 하나의 실행 문맥 */
        private final class Ctx implements NodeContext {
            private final PlanNode node;
            private final FlowMessage input;
            private final int hops;
            private final int attempt;
            private final List<Emission> emissions = new ArrayList<>();
            private final List<String> ports = new ArrayList<>();
            private final List<ExecutionReport.ActionRecord> actionRecords = new ArrayList<>();
            private String errorType;
            private String errorMessage;
            private boolean finalError;

            Ctx(PlanNode node, FlowMessage input, int hops, int attempt) {
                this.node = node;
                this.input = input;
                this.hops = hops;
                this.attempt = attempt;
            }

            @Override
            public UUID flowId() {
                return plan.flowId();
            }

            @Override
            public int flowVersion() {
                return plan.version();
            }

            @Override
            public long organizationId() {
                return plan.organizationId();
            }

            @Override
            public String nodeId() {
                return node.id();
            }

            @Override
            public Instant now() {
                return now;
            }

            @Override
            public boolean dryRun() {
                return dryRun;
            }

            @Override
            public int attempt() {
                return attempt;
            }

            @Override
            public boolean wired(String port) {
                return node.hasWires(port);
            }

            private String cacheKey(String targetKey) {
                return node.id() + "|" + targetKey;
            }

            @Override
            public JsonNode state(String targetKey) {
                String key = cacheKey(targetKey);
                if (!stateCache.containsKey(key)) {
                    StoredState s = store.lockState(plan.flowId(), plan.organizationId(), node.id(), targetKey);
                    stateCache.put(key, resolve(s));
                }
                JsonNode s = stateCache.get(key);
                return s == null ? null : s.deepCopy();
            }

            /** 상태 행의 지문이 지금 노드와 다르면 상태 정책을 적용한다(FLW-06.03) */
            private JsonNode resolve(StoredState s) {
                JsonNode state = s == null ? null : s.state();
                if (state == null || state.isEmpty()) {
                    return null;
                }
                JsonNode current = node.stateConfig();
                if (s.stateConfig() == null || current == null || s.stateConfig().equals(current)) {
                    return state;
                }
                StatePolicy policy = node.type().statePolicy(s.stateConfig(), current);
                return switch (policy) {
                    case KEEP -> state;
                    case MIGRATE -> {
                        JsonNode migrated = node.type().migrateState(s.stateConfig(), current, state);
                        yield migrated == null || migrated.isEmpty() ? null : migrated;
                    }
                    default -> null;
                };
            }

            @Override
            public void saveState(String targetKey, JsonNode state) {
                state(targetKey);   // 잠금을 먼저(위상 순서) 건다
                if (state != null) {
                    int bytes = state.toString().getBytes(StandardCharsets.UTF_8).length;
                    if (bytes > limits.maxStateBytes()) {
                        throw new StateTooLargeException("노드 상태가 대상 키당 " + limits.maxStateBytes() + "바이트를 넘습니다("
                                + bytes + "바이트)");
                    }
                }
                store.writeState(plan.flowId(), plan.organizationId(), node.id(), targetKey, state, node.stateConfig());
                stateCache.put(cacheKey(targetKey), state == null ? null : state.deepCopy());
            }

            @Override
            public long scheduleTimer(TimerKind kind, String targetKey, Instant dueAt, JsonNode context) {
                if (waitingTimers == null) {
                    waitingTimers = store.countWaitingTimers(plan.flowId(), plan.organizationId());
                }
                if (waitingTimers >= MAX_WAITING_TIMERS) {
                    throw new TimerLimitException("플로우의 대기 타이머가 " + MAX_WAITING_TIMERS + "개라 새 대기를 거부합니다(BR-FLW-16)");
                }
                ObjectNode c = context instanceof ObjectNode o ? o.deepCopy() : Jsons.object();
                if (!c.has(RUN_KEY)) {
                    ObjectNode r = c.putObject(RUN_KEY);
                    String key = input != null && input.runKey() != null ? input.runKey() : runKey;
                    if (key != null) {
                        r.put("key", key);
                    }
                    r.put("id", input != null ? input.triggerMessageId() : triggerMessageId);
                }
                long id = store.insertTimer(plan.flowId(), plan.organizationId(), plan.version(), node.id(), targetKey, kind, dueAt, c);
                waitingTimers++;
                return id;
            }

            @Override
            public void cancelTimer(long timerId) {
                store.cancelTimer(plan.organizationId(), timerId);
                if (waitingTimers != null && waitingTimers > 0) {
                    waitingTimers--;
                }
            }

            @Override
            public boolean action(ActionDraft action) {
                if (action.controlsDevices()) {
                    Optional<String> skip = options.guard().skipReason(plan.organizationId(), action, input);
                    if (skip.isPresent()) {
                        actionRecords.add(new ExecutionReport.ActionRecord(action.kind(), action.idempotencyKey(), dryRun,
                                skip.get(), action.summary()));
                        samples.add(new DebugSample(node.id(), input == null ? triggerMessageId : input.triggerMessageId(), "out",
                                null, Jsons.object().put("skipped", skip.get()).put("action", action.summary()), true));
                        return false;
                    }
                }
                actions.add(action);
                actionRecords.add(new ExecutionReport.ActionRecord(action.kind(), action.idempotencyKey(), dryRun, null,
                        action.summary()));
                if (dryRun) {
                    samples.add(new DebugSample(node.id(), input == null ? triggerMessageId : input.triggerMessageId(), "out", null,
                            Jsons.object().put("dryRun", action.summary()), true));
                    return true;
                }
                String trigger = input == null ? triggerMessageId : input.triggerMessageId();
                store.insertOutbox(plan.flowId(), plan.organizationId(), plan.version(), node.id(),
                        trigger == null ? action.idempotencyKey() : trigger, action);
                return true;
            }

            @Override
            public void emit(String port, FlowMessage message) {
                ports.add(port);
                FlowMessage m = message.runKey() == null && runKey != null ? message.withRunKey(runKey) : message;
                emissions.add(new Emission(port, m));
            }

            @Override
            public void fail(FlowMessage message, String errorType, String detail) {
                RetryPolicyHolder retry = new RetryPolicyHolder(node);
                if (input != null && !NOT_RETRYABLE.contains(errorType) && attempt <= retry.maxAttempts()) {
                    ObjectNode c = Jsons.object();
                    c.set("message", message.body());
                    c.put("triggerMessageId", message.triggerMessageId());
                    c.put("attempt", attempt + 1);
                    c.put("hops", hops);
                    c.put("lastError", errorType + ": " + detail);
                    try {
                        scheduleTimer(TimerKind.RETRY, message.targetKey(), now.plus(retry.delayAfter(attempt)), c);
                        this.errorType = errorType;
                        this.errorMessage = detail + " (재시도 " + attempt + "/" + retry.maxAttempts() + " 예약)";
                        ports.add("retry");
                        return;
                    } catch (TimerLimitException e) {
                        detail = detail + " (재시도 불가: " + e.getMessage() + ")";
                    }
                }
                finalFail(message, errorType, detail);
            }

            void finalFail(FlowMessage message, String type, String detail) {
                this.errorType = type;
                this.errorMessage = detail;
                this.finalError = true;
                ports.add(FlowNodeType.ERROR_PORT);
                if (node.hasWires(FlowNodeType.ERROR_PORT)) {
                    ObjectNode body = message.body().deepCopy();
                    ObjectNode error = body.putObject("error");
                    error.put("nodeId", node.id());
                    error.put("errorType", type);
                    error.put("message", detail);
                    error.put("attempts", attempt);
                    emissions.add(new Emission(FlowNodeType.ERROR_PORT, message.withBody(body)));
                } else {
                    ObjectNode note = Jsons.object();
                    note.put("errorType", type);
                    note.put("message", detail);
                    note.put("attempts", attempt);
                    samples.add(new DebugSample(node.id(), message.triggerMessageId(), "out", FlowNodeType.ERROR_PORT, note, true));
                    log.info("플로우 {} v{} 노드 {} 오류(error 포트 없음, 이 갈래만 끝냄): {} {}", plan.flowId(), plan.version(),
                            node.id(), type, detail);
                }
            }

            void failTimer(String type, String detail, TimerFire fire) {
                JsonNode m = fire.context() == null ? null : fire.context().get("message");
                FlowMessage message = new FlowMessage(m instanceof ObjectNode o ? o.deepCopy() : Jsons.object(),
                        triggerMessageId == null ? "timer:" + fire.timerId() : triggerMessageId, fire.targetKey(), runKey);
                finalFail(message, type, detail);
            }

            @Override
            public void debug(DebugSample sample) {
                samples.add(sample);
            }
        }
    }

    /** 노드의 재시도 정책(계획에 없으면 없음) */
    private record RetryPolicyHolder(PlanNode node) {
        int maxAttempts() {
            return node.retry() == null ? 0 : node.retry().maxAttempts();
        }

        java.time.Duration delayAfter(int attempt) {
            return node.retry().delayAfter(attempt);
        }
    }
}
