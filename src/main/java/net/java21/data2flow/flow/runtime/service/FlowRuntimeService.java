package net.java21.data2flow.flow.runtime.service;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.common.Backoff;
import net.java21.data2flow.flow.common.DeploymentScope;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.common.TransientFailures;
import net.java21.data2flow.flow.plan.domain.ExecutionPlan;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.domain.PlanNode;
import net.java21.data2flow.flow.plan.domain.TimerFire;
import net.java21.data2flow.flow.plan.domain.TriggerNode;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.domain.DebugSink;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.runtime.repository.PartitionProgressRepository;
import net.java21.data2flow.flow.timer.repository.TimerRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;

/**
 * 메시지·타이머를 플로우에 넣는 곳(FLW-05.01·05.02·05.03).
 *
 * <ul>
 *   <li><b>텔레메트리</b>: 조직의 실행 중인 플로우마다 트리거가 맞으면 플로우 하나당 트랜잭션 하나로 처리한다. 트랜잭션 안에서
 *       (플로우, 파티션) 처리 진행을 잠그고 이미 처리한 오프셋이면 건너뛴 뒤, 노드 상태·타이머·아웃박스·지표·처리 진행을 함께 커밋한다
 *       (BR-FLW-29). 스트림 오프셋은 호출하는 쪽(소비자)이 모든 플로우를 커밋한 뒤 저장한다.</li>
 *   <li><b>오류 격리</b>: 플로우마다 따로 커밋하므로 한 플로우가 실패해도 다른 플로우는 진행된다. DB 같은 일시 장애는 같은 메시지를
 *       계속 다시 시도하고(유실 0), 일시 장애가 아닌 실패가 3번이면 그 플로우만 건너뛴다(오류 지표·로그).</li>
 *   <li><b>지속 타이머</b>: 만기 후보를 고른 뒤 타이머마다 트랜잭션 하나로 ① 그 노드의 대상 키 상태를 잠그고 ② 타이머 행을
 *       {@code FOR UPDATE SKIP LOCKED}로 잠가(이미 발화·취소·다른 인스턴스가 잡았으면 건너뜀) ③ 현재 버전 계획으로 이어서 실행하고
 *       ④ FIRED로 바꿔 커밋한다. 인스턴스가 커밋 전에 죽으면 잠금이 풀려 다른 인스턴스가 한 번만 발화한다(TC-FLW-097·098).
 *       잠금 순서(상태 → 타이머)가 메시지 처리(상태 → 타이머 취소)와 같아 교착이 없다. 타이머를 만든 노드가 현재 버전에 없으면 취소한다
 *       (live-reload §4.3).</li>
 * </ul>
 */
public class FlowRuntimeService {

    private static final Logger log = LoggerFactory.getLogger(FlowRuntimeService.class);
    private static final int NON_TRANSIENT_ATTEMPTS = 3;

    private final FlowRegistry registry;
    private final FlowExecutor executor;
    private final JdbcExecutionStore store;
    private final PartitionProgressRepository progress;
    private final TimerRepository timers;
    private final TransactionTemplate tx;
    private final DebugSink debug;
    private final DeploymentScope scope;
    private final FlowEngineProperties properties;
    private final Clock clock;
    private final Counter skipped;

    public FlowRuntimeService(FlowRegistry registry, FlowExecutor executor, JdbcExecutionStore store,
                              PartitionProgressRepository progress, TimerRepository timers, TransactionTemplate tx,
                              DebugSink debug, DeploymentScope scope, FlowEngineProperties properties, Clock clock,
                              MeterRegistry meters) {
        this.registry = registry;
        this.executor = executor;
        this.store = store;
        this.progress = progress;
        this.timers = timers;
        this.tx = tx;
        this.debug = debug;
        this.scope = scope;
        this.properties = properties;
        this.clock = clock;
        this.skipped = Counter.builder("data2flow.flow.executions.skipped")
                .description("일시 장애가 아닌 실패로 건너뛴 (메시지, 플로우) 수").register(meters);
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
        for (LoadedFlow flow : registry.running(telemetry.organizationId())) {
            ExecutionPlan plan = flow.plan().acquire();
            try {
                List<Match> matches = new ArrayList<>();
                for (PlanNode trigger : plan.triggers()) {
                    Optional<FlowMessage> m = ((TriggerNode) trigger.compiled()).match(telemetry);
                    m.ifPresent(message -> matches.add(new Match(trigger, message)));
                }
                if (matches.isEmpty()) {
                    continue;
                }
                if (!runWithRetry(() -> executeTelemetry(flow, plan, matches, partition, offset), flow, stopping)) {
                    return false;
                }
            } finally {
                plan.release();
            }
        }
        registry.sweep();
        return true;
    }

    private record Match(PlanNode trigger, FlowMessage message) {
    }

    private List<ExecutionReport> executeTelemetry(LoadedFlow flow, ExecutionPlan plan, List<Match> matches, int partition,
                                                   long offset) {
        List<ExecutionReport> reports = tx.execute(status -> {
            if (progress.lockAndCheckProcessed(plan.organizationId(), plan.flowId(), partition, offset)) {
                return List.<ExecutionReport>of();   // 다시 읽은 메시지(BR-FLW-29)
            }
            List<ExecutionReport> out = new ArrayList<>();
            for (Match m : matches) {
                out.add(executor.runTrigger(plan, flow.overlay(), m.trigger(), m.message(), store, false));
            }
            progress.markProcessed(plan.organizationId(), plan.flowId(), partition, offset, clock.instant());
            return out;
        });
        publish(plan, reports);
        return reports;
    }

    private boolean runWithRetry(Runnable work, LoadedFlow flow, BooleanSupplier stopping) {
        Backoff backoff = new Backoff(properties.execution().retryInitial(), properties.execution().retryMax());
        int failures = 0;
        while (true) {
            try {
                work.run();
                return true;
            } catch (RuntimeException e) {
                if (TransientFailures.isTransient(e)) {
                    log.warn("플로우 {} 처리 중 일시 장애(다시 시도, 대기 {}): {}", flow.flowId(), backoff.current(), e.getMessage());
                } else if (++failures >= NON_TRANSIENT_ATTEMPTS) {
                    skipped.increment();
                    log.error("플로우 {} v{}에서 메시지를 {}번 처리하지 못해 이 플로우만 건너뜁니다", flow.flowId(), flow.version(),
                            failures, e);
                    return true;
                } else {
                    log.warn("플로우 {} 처리 오류({}회): {}", flow.flowId(), failures, e.toString());
                }
                if (!backoff.pause(stopping)) {
                    return false;
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
            Optional<LoadedFlow> flow = registry.get(c.flowId());
            if (flow.isEmpty() || !flow.get().running()) {
                continue;
            }
            try {
                if (fire(flow.get(), c)) {
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

    private boolean fire(LoadedFlow flow, TimerRepository.Candidate c) {
        ExecutionPlan plan = flow.plan().acquire();
        try {
            ExecutionReport report = tx.execute(status -> {
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
                ExecutionReport r = executor.runTimer(plan, flow.overlay(), node,
                        new TimerFire(t.id(), t.kind(), t.targetKey(), t.dueAt(), t.context(), t.flowVersion()), store, false);
                timers.markFired(t.organizationId(), t.id(), clock.instant());
                return r;
            });
            if (report == null) {
                return false;
            }
            publish(plan, List.of(report));
            return true;
        } finally {
            plan.release();
        }
    }

    private void publish(ExecutionPlan plan, List<ExecutionReport> reports) {
        if (reports == null) {
            return;
        }
        for (ExecutionReport r : reports) {
            if (!r.samples().isEmpty()) {
                try {
                    debug.publish(plan.flowId(), plan.version(), r.samples());
                } catch (RuntimeException e) {
                    log.debug("디버그 샘플 발행 실패(손실 허용): {}", e.getMessage());
                }
            }
        }
    }
}
