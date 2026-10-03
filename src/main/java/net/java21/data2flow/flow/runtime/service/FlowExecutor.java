package net.java21.data2flow.flow.runtime.service;

import net.java21.data2flow.contracts.flow.FlowNodeType;
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
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.runtime.domain.ExecutionStore;
import net.java21.data2flow.flow.runtime.domain.NodeMetrics;
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
import java.util.PriorityQueue;
import java.util.UUID;

/**
 * 실행 계획 하나로 메시지 하나(또는 타이머 발화 하나)를 끝까지 처리한다(FLW-05.01·05.02·05.03).
 *
 * <ul>
 *   <li><b>버전 고정</b>: 호출하는 쪽이 넘긴 계획 하나만 쓴다(BR-FLW-06).</li>
 *   <li><b>실행 순서</b>: 위상 순서(작은 것부터). 노드 상태 잠금도 이 순서로 걸려 같은 계획을 쓰는 실행끼리 교착이 없다.</li>
 *   <li><b>오류 격리</b>(FLW-05.03): 노드 예외·실패는 그 노드의 error 포트로 나가고, error 포트에 와이어가 없으면 그 갈래만 끝나고
 *       오류 지표가 1 오른다. 다른 갈래·다른 플로우·같은 기기의 다음 메시지는 계속 처리된다. 저장소(DB) 장애만 밖으로 던져
 *       트랜잭션을 되돌리고 다시 시도하게 한다(유실 0).</li>
 *   <li><b>한도</b>(BR-FLW-16): 메시지 하나가 거치는 노드 수 100, 노드 하나가 입력 하나에 내보내는 메시지 100, 노드 상태 256KB.</li>
 *   <li><b>바이패스</b>(BR-FLW-10): 변환·조건 노드는 입력을 첫 출력 포트로 넘기고, 행동 노드는 실행하지 않고 "bypassed"를 기록한다.</li>
 * </ul>
 */
public class FlowExecutor {

    private static final Logger log = LoggerFactory.getLogger(FlowExecutor.class);

    private final Clock clock;
    private final FlowEngineProperties.Execution limits;

    public FlowExecutor(Clock clock, FlowEngineProperties.Execution limits) {
        this.clock = clock;
        this.limits = limits;
    }

    /** 트리거가 받은 메시지를 처리한다 */
    public ExecutionReport runTrigger(ExecutionPlan plan, Overlay overlay, PlanNode trigger, FlowMessage message,
                                      ExecutionStore store, boolean dryRun) {
        Run run = new Run(plan, overlay, store, dryRun);
        run.enqueue(trigger, message, 0, null);
        run.drain();
        return run.finish();
    }

    /** 만기가 된 타이머를 그 노드의 {@code onTimer}로 넘기고 이어서 처리한다 */
    public ExecutionReport runTimer(ExecutionPlan plan, Overlay overlay, PlanNode node, TimerFire fire, ExecutionStore store,
                                    boolean dryRun) {
        Run run = new Run(plan, overlay, store, dryRun);
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

    private record Item(PlanNode node, FlowMessage message, int hops, long sequence) {
    }

    private final class Run {
        private final ExecutionPlan plan;
        private final Overlay overlay;
        private final ExecutionStore store;
        private final boolean dryRun;
        private final Instant now = clock.instant();
        private final PriorityQueue<Item> queue = new PriorityQueue<>(
                Comparator.comparingInt((Item i) -> i.node().order()).thenComparingLong(Item::sequence));
        private final Map<String, NodeMetrics> metrics = new LinkedHashMap<>();
        private final List<ExecutionReport.Step> steps = new ArrayList<>();
        private final List<ActionDraft> actions = new ArrayList<>();
        private final List<DebugSample> samples = new ArrayList<>();
        private final Map<String, JsonNode> stateCache = new HashMap<>();
        private long sequence;
        private int errors;

        Run(ExecutionPlan plan, Overlay overlay, ExecutionStore store, boolean dryRun) {
            this.plan = plan;
            this.overlay = overlay == null ? Overlay.NONE : overlay;
            this.store = store;
            this.dryRun = dryRun;
        }

        void enqueue(PlanNode node, FlowMessage message, int hops, String fromNode) {
            queue.add(new Item(node, message, hops, sequence++));
        }

        void drain() {
            while (!queue.isEmpty()) {
                process(queue.poll());
            }
        }

        void fire(PlanNode node, TimerFire fire) {
            Ctx ctx = new Ctx(node, null, 0);
            long started = System.nanoTime();
            try {
                node.compiled().onTimer(fire, ctx);
            } catch (DataAccessException e) {
                throw e;
            } catch (StateTooLargeException e) {
                ctx.failTimer("NODE_STATE_TOO_LARGE", e.getMessage(), fire);
            } catch (RuntimeException e) {
                log.warn("플로우 {} v{} 노드 {} 타이머 처리 오류: {}", plan.flowId(), plan.version(), node.id(), e.toString());
                ctx.failTimer("NODE_EXCEPTION", e.toString(), fire);
            }
            finishNode(node, ctx, started);
        }

        void process(Item item) {
            PlanNode node = item.node();
            Ctx ctx = new Ctx(node, item.message(), item.hops());
            long started = System.nanoTime();
            if (item.hops() >= limits.maxHops()) {
                ctx.fail(item.message(), "FLOW_HOP_LIMIT", "메시지 하나가 거치는 노드 수가 " + limits.maxHops() + "를 넘었습니다");
                finishNode(node, ctx, started);
                return;
            }
            if (overlay.debug().contains(node.id())) {
                samples.add(new DebugSample(node.id(), item.message().triggerMessageId(), "in", null, item.message().body(), true));
            }
            try {
                if (overlay.bypass().contains(node.id())) {
                    if (node.compiled().isAction()) {
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
            } catch (RuntimeException e) {
                log.warn("플로우 {} v{} 노드 {} 처리 오류: {}", plan.flowId(), plan.version(), node.id(), e.toString());
                ctx.fail(item.message(), "NODE_EXCEPTION", e.toString());
            }
            finishNode(node, ctx, started);
        }

        private void finishNode(PlanNode node, Ctx ctx, long started) {
            NodeMetrics m = metrics.computeIfAbsent(node.id(), k -> new NodeMetrics());
            m.processed((System.nanoTime() - started) / 1000);
            steps.add(new ExecutionReport.Step(node.id(), List.copyOf(ctx.ports), ctx.errorType, ctx.errorMessage,
                    ctx.emissions.stream().map(e -> new ExecutionReport.Output(e.port(), e.message().body())).toList()));
            if (ctx.errorType != null) {
                m.error();
                errors++;
            }
            for (int i = 0; i < ctx.commands; i++) {
                m.command();
            }
            int fanout = 0;
            for (Emission e : ctx.emissions) {
                if (++fanout > limits.maxFanout()) {
                    m.error();
                    errors++;
                    steps.add(new ExecutionReport.Step(node.id(), List.of(FlowNodeType.ERROR_PORT), "FLOW_FANOUT_LIMIT",
                            "노드 하나가 입력 하나에 내보내는 메시지가 " + limits.maxFanout() + "를 넘었습니다"));
                    break;
                }
                if (overlay.debug().contains(node.id())) {
                    samples.add(new DebugSample(node.id(), e.message().triggerMessageId(), "out", e.port(), e.message().body(), true));
                }
                List<String> targets = node.targets(e.port());
                for (int i = 0; i < targets.size(); i++) {
                    PlanNode next = plan.node(targets.get(i));
                    if (next != null) {
                        enqueue(next, i == 0 ? e.message() : e.message().copy(), ctx.hops + 1, node.id());
                    }
                }
            }
        }

        ExecutionReport finish() {
            Instant minute = now.truncatedTo(ChronoUnit.MINUTES);
            if (!dryRun) {
                metrics.forEach((nodeId, m) -> store.addMetrics(plan.flowId(), plan.organizationId(), nodeId, minute, m));
            }
            return new ExecutionReport(List.copyOf(steps), List.copyOf(actions), List.copyOf(samples), errors, Map.copyOf(metrics));
        }

        private record Emission(String port, FlowMessage message) {
        }

        /** 노드 하나의 실행 문맥 */
        private final class Ctx implements NodeContext {
            private final PlanNode node;
            private final FlowMessage input;
            private final int hops;
            private final List<Emission> emissions = new ArrayList<>();
            private final List<String> ports = new ArrayList<>();
            private String errorType;
            private String errorMessage;
            private int commands;

            Ctx(PlanNode node, FlowMessage input, int hops) {
                this.node = node;
                this.input = input;
                this.hops = hops;
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

            private String cacheKey(String targetKey) {
                return node.id() + "|" + targetKey;
            }

            @Override
            public JsonNode state(String targetKey) {
                String key = cacheKey(targetKey);
                if (!stateCache.containsKey(key)) {
                    JsonNode s = store.lockState(plan.flowId(), plan.organizationId(), node.id(), targetKey);
                    stateCache.put(key, s == null || s.isEmpty() ? null : s);
                }
                JsonNode s = stateCache.get(key);
                return s == null ? null : s.deepCopy();
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
                store.writeState(plan.flowId(), plan.organizationId(), node.id(), targetKey, state);
                stateCache.put(cacheKey(targetKey), state == null ? null : state.deepCopy());
            }

            @Override
            public long scheduleTimer(TimerKind kind, String targetKey, Instant dueAt, JsonNode context) {
                return store.insertTimer(plan.flowId(), plan.organizationId(), plan.version(), node.id(), targetKey, kind, dueAt,
                        context);
            }

            @Override
            public void cancelTimer(long timerId) {
                store.cancelTimer(plan.organizationId(), timerId);
            }

            @Override
            public void action(ActionDraft action) {
                actions.add(action);
                commands++;
                if (dryRun) {
                    samples.add(new DebugSample(node.id(), input == null ? null : input.triggerMessageId(), "out", null,
                            Jsons.object().put("dryRun", action.summary()), true));
                    return;
                }
                String trigger = input == null ? action.idempotencyKey() : input.triggerMessageId();
                store.insertOutbox(plan.flowId(), plan.organizationId(), plan.version(), node.id(), trigger, action);
            }

            @Override
            public void emit(String port, FlowMessage message) {
                ports.add(port);
                emissions.add(new Emission(port, message));
            }

            @Override
            public void fail(FlowMessage message, String errorType, String detail) {
                this.errorType = errorType;
                this.errorMessage = detail;
                ports.add(FlowNodeType.ERROR_PORT);
                if (node.hasWires(FlowNodeType.ERROR_PORT)) {
                    ObjectNode body = message.body().deepCopy();
                    ObjectNode error = body.putObject("error");
                    error.put("nodeId", node.id());
                    error.put("errorType", errorType);
                    error.put("message", detail);
                    error.put("attempts", 1);
                    emissions.add(new Emission(FlowNodeType.ERROR_PORT, message.withBody(body)));
                } else {
                    ObjectNode note = Jsons.object();
                    note.put("errorType", errorType);
                    note.put("message", detail);
                    samples.add(new DebugSample(node.id(), message.triggerMessageId(), "out", FlowNodeType.ERROR_PORT, note, true));
                    log.info("플로우 {} v{} 노드 {} 오류(error 포트 없음, 이 갈래만 끝냄): {} {}", plan.flowId(), plan.version(),
                            node.id(), errorType, detail);
                }
            }

            void failTimer(String type, String detail, TimerFire fire) {
                JsonNode m = fire.context() == null ? null : fire.context().get("message");
                FlowMessage message = new FlowMessage(m instanceof ObjectNode o ? o.deepCopy() : Jsons.object(),
                        "timer:" + fire.timerId(), fire.targetKey());
                fail(message, type, detail);
            }

            @Override
            public void debug(DebugSample sample) {
                samples.add(sample);
            }
        }
    }
}
