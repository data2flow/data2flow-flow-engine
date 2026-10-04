package net.java21.data2flow.flow.runtime.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.common.Backoff;
import net.java21.data2flow.flow.common.DeploymentScope;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.common.TransientFailures;
import net.java21.data2flow.flow.plan.domain.ExecutionMode;
import net.java21.data2flow.flow.plan.domain.ExecutionPlan;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.domain.PlanNode;
import net.java21.data2flow.flow.plan.domain.TimerFire;
import net.java21.data2flow.flow.plan.domain.TimerKind;
import net.java21.data2flow.flow.plan.domain.TriggerNode;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.domain.ActionGuard;
import net.java21.data2flow.flow.runtime.domain.DebugSink;
import net.java21.data2flow.flow.runtime.domain.ExecutionListener;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.runtime.domain.NodeMetrics;
import net.java21.data2flow.flow.runtime.repository.BufferedTriggerRepository;
import net.java21.data2flow.flow.runtime.repository.PartitionProgressRepository;
import net.java21.data2flow.flow.timer.repository.TimerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

/**
 * 메시지·타이머·제어 결과를 플로우에 넣는 곳(FLW-05.01·05.02·05.03·05.04·05.07·06.02·08.04).
 *
 * <ul>
 *   <li><b>버전 고정</b>(BR-FLW-06): 플로우마다 {@link FlowRegistry#pin}으로 그때의 계획 하나를 잡아 그 메시지를 끝까지 처리하고 놓는다.
 *       라이브 리로드가 계획을 바꿔도 잡은 계획은 드레인이 끝날 때까지 닫히지 않는다.</li>
 *   <li><b>텔레메트리</b>: 조직의 적재된 플로우마다 트리거가 맞으면 플로우 하나당 트랜잭션 하나로 처리한다. 노드 상태·타이머·아웃박스를 쓰고
 *       마지막에 (플로우, 파티션) 처리 진행을 이 오프셋으로 올린다. 이미 처리한 오프셋이면(다시 읽은 메시지) 진행을 올리지 못하므로 트랜잭션을
 *       되돌려 상태·행동이 두 번 반영되지 않는다(BR-FLW-29). 스트림 오프셋은 호출하는 쪽(소비자)이 모든 플로우를 커밋한 뒤 저장한다.</li>
 *   <li><b>실행 모드</b>(BR-FLW-18): single은 진행 중인 실행이 있으면 버리고(버린 트리거 지표), restart는 진행 중인 실행의 타이머를 취소하고,
 *       parallel은 동시 실행 수가 차면 보관했다가(QUEUED) 실행 하나가 끝나면 순서대로 시작한다.</li>
 *   <li><b>일시 정지</b>(BR-FLW-25): DROP은 버리고 세며, BUFFER는 1시간까지 보관했다가 재개되면 받은 순서대로 처리한다.</li>
 *   <li><b>안전장치</b>(BR-FLW-14): 초당 실행 한도·순환이면 엔진이 플로우를 멈춘다({@link FlowSafetyGuard}).</li>
 *   <li><b>오류 격리</b>: 플로우마다 따로 커밋하므로 한 플로우가 실패해도 다른 플로우는 진행된다. DB 같은 일시 장애는 같은 메시지를
 *       계속 다시 시도하고(유실 0), 일시 장애가 아닌 실패가 3번이면 그 플로우만 건너뛴다(오류 지표·로그).</li>
 *   <li><b>지속 타이머</b>: 만기 후보를 고른 뒤 타이머마다 트랜잭션 하나로 ① 그 노드의 대상 키 상태를 잠그고 ② 타이머 행을
 *       {@code FOR UPDATE SKIP LOCKED}로 잠가 ③ 현재 버전 계획으로 이어서 실행하고 ④ FIRED로 바꿔 커밋한다(TC-FLW-097·098).</li>
 *   <li><b>제어 결과</b>(action.control ok·failed): EVT-ACT-01 끝 상태가 오면 멱등 키로 결과 대기 타이머를 찾아 같은 방식으로 이어서 실행한다.</li>
 *   <li>커밋된 실행은 {@link ExecutionListener}(추적·라이브 뷰·오류율)에 알린다.</li>
 * </ul>
 */
public class FlowRuntimeService {

    private static final Logger log = LoggerFactory.getLogger(FlowRuntimeService.class);
    private static final int NON_TRANSIENT_ATTEMPTS = 3;
    /** 일시 정지 BUFFER·parallel 대기 보관 기한(BR-FLW-25) */
    public static final Duration BUFFER_RETENTION = Duration.ofHours(1);

    private final FlowRegistry registry;
    private final FlowExecutor executor;
    private final JdbcExecutionStore store;
    private final PartitionProgressRepository progress;
    private final TimerRepository timers;
    private final BufferedTriggerRepository buffered;
    private final TransactionTemplate tx;
    private final DebugSink debug;
    private final DeploymentScope scope;
    private final FlowEngineProperties properties;
    private final Clock clock;
    private final FlowSafetyGuard safety;
    private final ActionGuard guard;
    private final List<ExecutionListener> listeners = new CopyOnWriteArrayList<>();
    private final Counter skipped;

    public FlowRuntimeService(FlowRegistry registry, FlowExecutor executor, JdbcExecutionStore store,
                              PartitionProgressRepository progress, TimerRepository timers, BufferedTriggerRepository buffered,
                              TransactionTemplate tx, DebugSink debug, DeploymentScope scope, FlowEngineProperties properties,
                              Clock clock, MeterRegistry meters, FlowSafetyGuard safety, ActionGuard guard) {
        this.registry = registry;
        this.executor = executor;
        this.store = store;
        this.progress = progress;
        this.timers = timers;
        this.buffered = buffered;
        this.tx = tx;
        this.debug = debug;
        this.scope = scope;
        this.properties = properties;
        this.clock = clock;
        this.safety = safety;
        this.guard = guard == null ? ActionGuard.NONE : guard;
        this.skipped = Counter.builder("data2flow.flow.executions.skipped")
                .description("일시 장애가 아닌 실패로 건너뛴 (메시지, 플로우) 수").register(meters);
    }

    /** 커밋된 실행을 받을 곳을 더한다(추적·라이브 뷰·시험) */
    public void addListener(ExecutionListener listener) {
        listeners.add(listener);
    }

    public void removeListener(ExecutionListener listener) {
        listeners.remove(listener);
    }

    private FlowExecutor.RunOptions live() {
        return new FlowExecutor.RunOptions(false, null, guard);
    }

    /** 묶음 처리의 텔레메트리 하나(스트림 오프셋과 함께) */
    public record TelemetryItem(CanonicalTelemetry telemetry, long offset) {
    }

    /**
     * 텔레메트리 하나를 관심 있는 모든 플로우에 넣는다.
     *
     * @param partition 스트림 파티션
     * @param offset    스트림 오프셋
     * @param stopping  종료 신호(일시 장애 재시도를 멈춤)
     * @return 모두 처리(또는 건너뜀)했으면 true. 종료 중이라 그만두면 false(오프셋을 저장하지 않는다)
     */
    public boolean processTelemetry(CanonicalTelemetry telemetry, int partition, long offset, BooleanSupplier stopping) {
        return processBatch(partition, List.of(new TelemetryItem(telemetry, offset)), stopping);
    }

    /**
     * 같은 파티션의 텔레메트리 여러 건(오프셋 순서)을 넣는다. 밀린 메시지가 있을 때 파티션 작업자가 모아 부르며, 플로우마다 트랜잭션 하나로 처리해
     * 커밋 수를 줄인다(TC-FLW-134 초당 200건). 메시지마다 계획을 따로 잡지 않고 묶음 전체가 그 플로우의 계획 하나를 쓴다: 한 메시지는 여전히
     * 버전 하나로만 처리된다(BR-FLW-06). 일시 장애가 아닌 실패가 반복되면 그 플로우만 메시지 하나씩 다시 처리해 실패한 메시지만 건너뛴다.
     *
     * @return 모두 처리(또는 건너뜀)했으면 true. 종료 중이라 그만두면 false
     */
    public boolean processBatch(int partition, List<TelemetryItem> items, BooleanSupplier stopping) {
        java.util.Map<Long, List<TelemetryItem>> byOrganization = new java.util.LinkedHashMap<>();
        for (TelemetryItem item : items) {
            byOrganization.computeIfAbsent(item.telemetry().organizationId(), k -> new ArrayList<>()).add(item);
        }
        for (var entry : byOrganization.entrySet()) {
            for (LoadedFlow candidate : registry.loaded(entry.getKey())) {
                Optional<LoadedFlow> pinned = registry.pin(candidate.flowId());
                if (pinned.isEmpty()) {
                    continue;
                }
                LoadedFlow flow = pinned.get();
                try {
                    if (!processFlow(flow, partition, entry.getValue(), stopping)) {
                        return false;
                    }
                } finally {
                    flow.plan().release();
                }
            }
        }
        registry.sweep();
        return true;
    }

    /** 맞는 트리거가 있는 메시지 하나 */
    private record Pending(TelemetryItem item, List<Match> matches) {
    }

    private boolean processFlow(LoadedFlow flow, int partition, List<TelemetryItem> items, BooleanSupplier stopping) {
        ExecutionPlan plan = flow.plan();
        List<Pending> pending = new ArrayList<>();
        int unsafe = 0;
        boolean running = flow.running();
        for (TelemetryItem item : items) {
            List<Match> matches = new ArrayList<>();
            for (PlanNode trigger : plan.triggers()) {
                Optional<FlowMessage> m = ((TriggerNode) trigger.compiled()).match(item.telemetry());
                m.ifPresent(message -> matches.add(new Match(trigger, message.withRunKey(plan.mode().runKey(message)))));
            }
            if (matches.isEmpty()) {
                continue;
            }
            if (running && safety != null && (safety.cycle(flow, item.telemetry()) || !safety.admit(flow))) {
                running = false;   // 엔진이 이 플로우를 멈췄다: 이 묶음의 남은 메시지도 실행하지 않는다
                unsafe += matches.size();
                continue;
            }
            if (!running && flow.running()) {
                unsafe += matches.size();
                continue;
            }
            pending.add(new Pending(item, matches));
        }
        if (unsafe > 0) {
            dropped(flow, "SAFETY_PAUSED", unsafe);
        }
        if (pending.isEmpty()) {
            return true;
        }
        if (!flow.running()) {
            if (flow.paused() && "BUFFER".equals(flow.pauseMode())) {
                return runWithRetry(() -> bufferPaused(flow, pending, partition), flow, stopping) != Outcome.STOPPED;
            }
            dropped(flow, "PAUSED", pending.stream().mapToInt(p -> p.matches().size()).sum());
            return true;
        }
        Outcome outcome = runWithRetry(() -> executeTelemetry(flow, pending, partition), flow, stopping);
        if (outcome == Outcome.SKIPPED && pending.size() > 1) {
            // 묶음이 계속 실패하면 하나씩: 실패하는 메시지만 건너뛴다
            for (Pending p : pending) {
                if (runWithRetry(() -> executeTelemetry(flow, List.of(p), partition), flow, stopping) == Outcome.STOPPED) {
                    return false;
                }
            }
            return true;
        }
        return outcome != Outcome.STOPPED;
    }

    private record Match(PlanNode trigger, FlowMessage message) {
    }

    private void executeTelemetry(LoadedFlow flow, List<Pending> pending, int partition) {
        ExecutionPlan plan = flow.plan();
        List<String> skippedModes = new ArrayList<>();
        long lastOffset = pending.getLast().item().offset();
        List<ExecutionReport> reports = tx.execute(status -> {
            skippedModes.clear();
            // 묶음이면 워터마크를 잠가 이미 처리한 메시지를 거른다(재시작 뒤 묶음 경계가 달라도 두 번 반영하지 않음)
            long watermark = pending.size() > 1 ? progress.lockWatermark(plan.organizationId(), plan.flowId(), partition) : -1;
            List<ExecutionReport> out = new ArrayList<>();
            for (Pending p : pending) {
                if (p.item().offset() <= watermark) {
                    continue;
                }
                for (Match m : p.matches()) {
                    if (!admitByMode(plan, m.trigger(), m.message(), skippedModes)) {
                        continue;
                    }
                    out.add(executor.runTrigger(plan, flow.overlay(), m.trigger(), m.message(), store, live()));
                }
            }
            // 처리 진행을 마지막에 올린다. 이미 처리한 오프셋(다시 읽은 메시지)이면 되돌린다(BR-FLW-29)
            if (progress.advance(plan.organizationId(), plan.flowId(), partition, lastOffset, clock.instant()) == 0) {
                status.setRollbackOnly();
                skippedModes.clear();
                return List.<ExecutionReport>of();
            }
            return out;
        });
        if (!skippedModes.isEmpty()) {
            dropped(flow, "MODE_" + skippedModes.getFirst(), skippedModes.size());
        }
        committed(flow, reports);
    }

    /**
     * 실행 모드 판정(트랜잭션 안). 실행하면 true. single은 버리고, parallel이 가득 차면 QUEUED로 보관한다.
     */
    private boolean admitByMode(ExecutionPlan plan, PlanNode trigger, FlowMessage message, List<String> skippedModes) {
        ExecutionMode mode = plan.mode();
        String runKey = message.runKey();
        switch (mode.concurrency()) {
            case SINGLE -> {
                if (timers.countRunsInProgress(plan.organizationId(), plan.flowId(), runKey) > 0) {
                    skippedModes.add("SINGLE");
                    log.debug("플로우 {} 실행 키 {}: single 모드로 건너뜀", plan.flowId(), runKey);
                    return false;
                }
            }
            case RESTART -> {
                int cancelled = timers.cancelRuns(plan.organizationId(), plan.flowId(), runKey);
                if (cancelled > 0) {
                    log.debug("플로우 {} 실행 키 {}: restart 모드로 진행 중 타이머 {}개 취소", plan.flowId(), runKey, cancelled);
                }
            }
            case PARALLEL -> {
                if (timers.countRunsInProgress(plan.organizationId(), plan.flowId(), runKey) >= mode.max()) {
                    buffered.insert(plan.organizationId(), plan.flowId(), runKey, "QUEUED", triggerJson(trigger, message),
                            clock.instant());
                    return false;
                }
            }
            default -> {
                // queued: 파티션 순서대로 처리
            }
        }
        return true;
    }

    private static ObjectNode triggerJson(PlanNode trigger, FlowMessage message) {
        ObjectNode j = Jsons.object();
        j.put("triggerNodeId", trigger.id());
        j.set("body", message.body());
        j.put("triggerMessageId", message.triggerMessageId());
        j.put("targetKey", message.targetKey());
        if (message.runKey() != null) {
            j.put("runKey", message.runKey());
        }
        return j;
    }

    private static FlowMessage messageOf(JsonNode j) {
        JsonNode body = j.get("body");
        return new FlowMessage(body instanceof ObjectNode o ? o.deepCopy() : Jsons.object(), j.path("triggerMessageId").asString(),
                j.path("targetKey").asString("*"), j.path("runKey").asString(null));
    }

    private static PlanNode triggerOf(ExecutionPlan plan, JsonNode j) {
        PlanNode node = plan.node(j.path("triggerNodeId").asString(""));
        return node != null ? node : plan.triggers().isEmpty() ? null : plan.triggers().getFirst();
    }

    /** 일시 정지 BUFFER: 처리 진행과 같은 트랜잭션에 보관한다(재처리해도 한 번만) */
    private void bufferPaused(LoadedFlow flow, List<Pending> pending, int partition) {
        ExecutionPlan plan = flow.plan();
        tx.executeWithoutResult(status -> {
            long watermark = pending.size() > 1 ? progress.lockWatermark(plan.organizationId(), plan.flowId(), partition) : -1;
            for (Pending p : pending) {
                if (p.item().offset() <= watermark) {
                    continue;
                }
                for (Match m : p.matches()) {
                    buffered.insert(plan.organizationId(), plan.flowId(), m.message().targetKey(), "PAUSED",
                            triggerJson(m.trigger(), m.message()), clock.instant());
                }
            }
            if (progress.advance(plan.organizationId(), plan.flowId(), partition, pending.getLast().item().offset(),
                    clock.instant()) == 0) {
                status.setRollbackOnly();
            }
        });
    }

    private void dropped(LoadedFlow flow, String reason, int count) {
        NodeMetrics m = new NodeMetrics();
        for (int i = 0; i < count; i++) {
            m.drop();
        }
        try {
            store.addMetrics(flow.flowId(), flow.organizationId(), NodeMetrics.FLOW_NODE,
                    clock.instant().truncatedTo(ChronoUnit.MINUTES), m);
        } catch (RuntimeException e) {
            log.debug("버린 트리거 지표 기록 실패: {}", e.getMessage());
        }
        for (ExecutionListener l : listeners) {
            try {
                l.dropped(flow, reason, count);
            } catch (RuntimeException e) {
                log.debug("실행 알림 실패: {}", e.getMessage());
            }
        }
    }

    private enum Outcome { DONE, SKIPPED, STOPPED }

    private Outcome runWithRetry(Runnable work, LoadedFlow flow, BooleanSupplier stopping) {
        Backoff backoff = new Backoff(properties.execution().retryInitial(), properties.execution().retryMax());
        int failures = 0;
        while (true) {
            try {
                work.run();
                return Outcome.DONE;
            } catch (RuntimeException e) {
                if (TransientFailures.isTransient(e)) {
                    log.warn("플로우 {} 처리 중 일시 장애(다시 시도, 대기 {}): {}", flow.flowId(), backoff.current(), e.getMessage());
                } else if (++failures >= NON_TRANSIENT_ATTEMPTS) {
                    skipped.increment();
                    log.error("플로우 {} v{}에서 메시지를 {}번 처리하지 못해 이 플로우만 건너뜁니다", flow.flowId(), flow.version(),
                            failures, e);
                    return Outcome.SKIPPED;
                } else {
                    log.warn("플로우 {} 처리 오류({}회): {}", flow.flowId(), failures, e.toString());
                }
                if (!backoff.pause(stopping)) {
                    return Outcome.STOPPED;
                }
            }
        }
    }

    /**
     * 만기가 된 지속 타이머를 발화한다(이 배포 조직·실행 중인 플로우만).
     *
     * @return 발화한 수
     */
    public int fireDueTimers(int limit, BooleanSupplier stopping) {
        List<TimerRepository.Candidate> due = timers.findDue(scope.organizations(), registry.runningIds(), clock.instant(), limit);
        int fired = 0;
        for (TimerRepository.Candidate c : due) {
            if (stopping.getAsBoolean()) {
                break;
            }
            try {
                if (fire(c, null)) {
                    fired++;
                }
            } catch (RuntimeException e) {
                log.warn("타이머 {} 발화 실패: {}", c.id(), e.toString());
                if (!TransientFailures.isTransient(e)) {
                    try {
                        timers.recordFailure(c.organizationId(), c.id(), properties.timer().maxAttempts());
                    } catch (RuntimeException ignored) {
                        // 다음 주기에 다시
                    }
                }
            }
        }
        registry.sweep();
        return fired;
    }

    /**
     * 제어 결과(EVT-ACT-01 끝 상태)를 결과 대기 타이머에 이어 붙인다(action.control ok·failed 포트).
     *
     * @param awaitKey 행동 요청 멱등 키
     * @param result   {@code {status, reason?, message?, commandId?, at}}
     * @return 이어서 실행한 수(기다리는 타이머가 없으면 0: 결과를 기다리지 않는 명령이거나 이미 처리됨)
     */
    public int resumeAwaiting(long organizationId, String awaitKey, JsonNode result) {
        int resumed = 0;
        for (TimerRepository.Candidate c : timers.findAwaiting(organizationId, awaitKey)) {
            if (fire(c, result)) {
                resumed++;
            }
        }
        registry.sweep();
        return resumed;
    }

    private boolean fire(TimerRepository.Candidate c, JsonNode result) {
        Optional<LoadedFlow> pinned = registry.pin(c.flowId());
        if (pinned.isEmpty()) {
            return false;
        }
        LoadedFlow flow = pinned.get();
        ExecutionPlan plan = flow.plan();
        try {
            if (!flow.running()) {
                return false;
            }
            List<ExecutionReport> reports = tx.execute(status -> {
                PlanNode node = plan.node(c.nodeId());
                if (node != null) {
                    store.lockState(plan.flowId(), plan.organizationId(), c.nodeId(), c.targetKey());   // ① 상태 잠금
                }
                Optional<TimerRepository.TimerRow> row = timers.lockWaiting(c.organizationId(), c.id());   // ② SKIP LOCKED
                if (row.isEmpty()) {
                    return null;
                }
                TimerRepository.TimerRow t = row.get();
                if (node == null) {
                    log.info("타이머 {}의 노드 {}가 현재 버전 v{}에 없어 취소합니다", t.id(), t.nodeId(), plan.version());
                    timers.markCancelled(t.organizationId(), t.id());
                    return null;
                }
                JsonNode context = t.context();
                if (result != null) {
                    ObjectNode c2 = context instanceof ObjectNode o ? o.deepCopy() : Jsons.object();
                    c2.set("result", result);
                    context = c2;
                }
                List<ExecutionReport> out = new ArrayList<>();
                out.add(executor.runTimer(plan, flow.overlay(), node,
                        new TimerFire(t.id(), t.kind(), t.targetKey(), t.dueAt(), context, t.flowVersion()), store, live()));
                timers.markFired(t.organizationId(), t.id(), clock.instant());
                if (TimerKind.RUN_KINDS.contains(t.kind()) && plan.mode().concurrency() == ExecutionMode.Concurrency.PARALLEL) {
                    out.addAll(dequeueParallel(flow, context.path(FlowExecutor.RUN_KEY).path("key").asString(null)));
                }
                return out;
            });
            if (reports == null) {
                return false;
            }
            committed(flow, reports);
            return true;
        } finally {
            plan.release();
        }
    }

    /** parallel: 실행 하나가 끝났으면 기다리던 트리거를 동시 실행 수가 찰 때까지 시작한다(트랜잭션 안) */
    private List<ExecutionReport> dequeueParallel(LoadedFlow flow, String runKey) {
        List<ExecutionReport> out = new ArrayList<>();
        if (runKey == null) {
            return out;
        }
        ExecutionPlan plan = flow.plan();
        Instant expiredBefore = clock.instant().minus(BUFFER_RETENTION);
        while (timers.countRunsInProgress(plan.organizationId(), plan.flowId(), runKey) < plan.mode().max()) {
            List<BufferedTriggerRepository.Row> rows = buffered.lockOldest(plan.organizationId(), plan.flowId(), "QUEUED", runKey, 1);
            if (rows.isEmpty()) {
                break;
            }
            BufferedTriggerRepository.Row row = rows.getFirst();
            buffered.delete(row.organizationId(), row.id());
            PlanNode trigger = triggerOf(plan, row.message());
            if (trigger == null || row.receivedAt().isBefore(expiredBefore)) {
                continue;
            }
            out.add(executor.runTrigger(plan, flow.overlay(), trigger, messageOf(row.message()), store, live()));
        }
        return out;
    }

    /**
     * 재개된 플로우의 보관 트리거(일시 정지 BUFFER)를 받은 순서대로 처리한다(BR-FLW-25). 한 플로우는 한 인스턴스만 비운다(권고 잠금).
     *
     * @return 처리(또는 기한 지나 버린) 수
     */
    public int drainBuffered(int limit, BooleanSupplier stopping) {
        int total = 0;
        for (UUID flowId : buffered.flowsWithBuffered(scope.organizations(), "PAUSED")) {
            if (stopping.getAsBoolean()) {
                break;
            }
            Optional<LoadedFlow> pinned = registry.pin(flowId);
            if (pinned.isEmpty()) {
                continue;
            }
            LoadedFlow flow = pinned.get();
            try {
                if (!flow.running()) {
                    continue;
                }
                int[] expired = {0};
                List<ExecutionReport> reports = tx.execute(status -> {
                    List<ExecutionReport> out = new ArrayList<>();
                    if (!buffered.tryLockFlow(flowId)) {
                        return out;
                    }
                    Instant expiredBefore = clock.instant().minus(BUFFER_RETENTION);
                    for (BufferedTriggerRepository.Row row : buffered.lockOldest(flow.organizationId(), flowId, "PAUSED", null, limit)) {
                        buffered.delete(row.organizationId(), row.id());
                        PlanNode trigger = triggerOf(flow.plan(), row.message());
                        if (trigger == null || row.receivedAt().isBefore(expiredBefore)) {
                            expired[0]++;
                            continue;
                        }
                        out.add(executor.runTrigger(flow.plan(), flow.overlay(), trigger, messageOf(row.message()), store, live()));
                    }
                    return out;
                });
                if (expired[0] > 0) {
                    dropped(flow, "BUFFER_EXPIRED", expired[0]);
                }
                total += expired[0] + (reports == null ? 0 : reports.size());
                committed(flow, reports);
            } finally {
                flow.plan().release();
            }
        }
        return total;
    }

    /** 보관 기한(1시간)이 지난 트리거를 버리고 버린 수를 지표에 남긴다(정리 작업) */
    public int expireBuffered() {
        Map<UUID, Long> expired = buffered.deleteExpired(clock.instant().minus(BUFFER_RETENTION));
        expired.forEach((flowId, count) -> registry.get(flowId).ifPresent(f -> dropped(f, "BUFFER_EXPIRED", count.intValue())));
        return expired.values().stream().mapToInt(Long::intValue).sum();
    }

    private void committed(LoadedFlow flow, List<ExecutionReport> reports) {
        if (reports == null) {
            return;
        }
        for (ExecutionReport r : reports) {
            if (!r.samples().isEmpty()) {
                try {
                    debug.publish(flow.flowId(), r.version(), r.samples());
                } catch (RuntimeException e) {
                    log.debug("디버그 샘플 발행 실패(손실 허용): {}", e.getMessage());
                }
            }
            for (ExecutionListener l : listeners) {
                try {
                    l.executed(flow, r);
                } catch (RuntimeException e) {
                    log.debug("실행 알림 실패: {}", e.getMessage());
                }
            }
        }
    }
}
