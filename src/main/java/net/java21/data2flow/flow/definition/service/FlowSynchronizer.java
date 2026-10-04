package net.java21.data2flow.flow.definition.service;

import net.java21.data2flow.flow.apply.service.ApplyReporter;
import net.java21.data2flow.flow.common.DeploymentScope;
import net.java21.data2flow.flow.definition.domain.FlowRuntime;
import net.java21.data2flow.flow.definition.domain.RuntimeSnapshot;
import net.java21.data2flow.flow.plan.domain.FlowValidationError;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.plan.service.StatePolicyResolver;
import net.java21.data2flow.flow.runtime.service.FlowSafetyGuard;
import net.java21.data2flow.flow.runtime.repository.NodeStateRepository;
import net.java21.data2flow.flow.timer.repository.TimerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * 라이브 리로드(design/flow-engine-and-live-reload.md §4, FLW-01.06 배포, FLW-06.01의 바탕). 원천은 core DB이고 엔진은 내부 API로 읽는다.
 *
 * <ol>
 *   <li>① 정의 로드: 시작·재연결·설정 변경(EVT-FLW-04, {@code data2flow.config} FLOW·OVERLAY) 때 API-FLW-81, 30초마다 API-FLW-80
 *       ({@code sinceVersion}, 바뀐 것이 없으면 204)</li>
 *   <li>② 컴파일: 실패하면 이전 버전을 그대로 두고 오류를 보고한다</li>
 *   <li>④ {@link FlowRegistry#put}으로 원자적 전환(이후 들어오는 메시지부터 새 버전)</li>
 *   <li>⑤ 상태 정리: 새 버전에 없는 노드의 상태는 24시간 보관(롤백하면 복원, BR-FLW-07)</li>
 *   <li>⑥ 이전 계획은 처리 중인 메시지가 끝나면 버린다</li>
 *   <li>⑦ 적용 보고(EVT-FLW-02)</li>
 * </ol>
 * 상태가 DISABLED·DELETED(목록에서 빠짐·404)이면 내리고 대기 타이머를 취소한다(BR-FLW-18). PAUSED는 계획을 두되 트리거·타이머를 멈춘다.
 * 설정이 바뀐 노드의 상태는 노드 종류의 정책(KEEP/RESET/MIGRATE, FLW-06.03)을 따른다: 상태 행에 쓴 노드의 지문이 있어 읽을 때 적용하고,
 * RESET 노드는 적용 직후 이전 설정의 상태 행과 판정 타이머를 정리한다. 엔진이 멈춘 플로우(폭주·순환)는 core가 PAUSED를 알려 줄 때까지 멈춘
 * 채로 둔다({@link FlowSafetyGuard#holdsPause}).
 */
public class FlowSynchronizer {

    private static final Logger log = LoggerFactory.getLogger(FlowSynchronizer.class);

    private final CoreFlowDirectory core;
    private final FlowCompiler compiler;
    private final FlowRegistry registry;
    private final ApplyReporter reporter;
    private final NodeStateRepository states;
    private final TimerRepository timers;
    private final DeploymentScope scope;
    private final Duration retainDeleted;
    private final Clock clock;
    private final FlowSafetyGuard safety;
    private volatile Long lastVersion;
    private volatile boolean synced;

    public FlowSynchronizer(CoreFlowDirectory core, FlowCompiler compiler, FlowRegistry registry, ApplyReporter reporter,
                            NodeStateRepository states, TimerRepository timers, DeploymentScope scope, Duration retainDeleted,
                            Clock clock, FlowSafetyGuard safety) {
        this.core = core;
        this.compiler = compiler;
        this.registry = registry;
        this.reporter = reporter;
        this.states = states;
        this.timers = timers;
        this.scope = scope;
        this.retainDeleted = retainDeleted;
        this.clock = clock;
        this.safety = safety;
    }

    /** 처음 전체 동기화가 끝났는지(readiness) */
    public boolean synced() {
        return synced;
    }

    /** 전체 동기화. {@code force}면 sinceVersion 없이(재연결) */
    public synchronized void syncAll(boolean force) {
        Optional<RuntimeSnapshot> snapshot = core.runtime(force ? null : lastVersion);
        if (snapshot.isEmpty()) {
            synced = true;
            return;
        }
        RuntimeSnapshot s = snapshot.get();
        if (s.organizationId() != null) {
            scope.add(s.organizationId());
        }
        Set<UUID> present = new HashSet<>();
        for (FlowRuntime flow : s.flows()) {
            present.add(flow.flowId());
            apply(flow);
        }
        for (UUID id : Set.copyOf(registry.ids())) {
            if (!present.contains(id)) {
                unload(id, "목록에서 빠짐(DISABLED·DELETED)");
            }
        }
        lastVersion = s.version();
        synced = true;
    }

    /** 플로우 하나(설정 변경 수신) */
    public synchronized void syncFlow(UUID flowId) {
        Optional<FlowRuntime> flow = core.flow(flowId);
        if (flow.isEmpty()) {
            unload(flowId, "실행 대상 아님(404)");
            return;
        }
        apply(flow.get());
    }

    /** 실행 대상 정보 하나를 적용한다 */
    public synchronized void apply(FlowRuntime flow) {
        scope.add(flow.organizationId());
        if (!flow.loadable()) {
            unload(flow.flowId(), "상태 " + flow.status());
            return;
        }
        String status = safety != null && safety.holdsPause(flow.flowId(), flow.status(), flow.activeVersion())
                ? "PAUSED" : flow.status();
        Optional<LoadedFlow> current = registry.get(flow.flowId());
        if (current.isPresent() && current.get().version() == flow.activeVersion()) {
            LoadedFlow c = current.get();
            boolean overlayChanged = c.overlay().revision() != flow.overlay().revision();
            LoadedFlow next = new LoadedFlow(c.flowId(), c.organizationId(), flow.name(), status, c.plan(), flow.overlay(),
                    flow.kind(), flow.rateLimitPerSec(), flow.pauseMode());
            if (!Objects.equals(c.name(), next.name()) || !c.status().equals(next.status()) || !c.overlay().equals(next.overlay())
                    || !c.kind().equals(next.kind()) || c.rateLimitPerSec() != next.rateLimitPerSec()
                    || !c.pauseMode().equals(next.pauseMode())) {
                registry.put(next);
                if (!c.status().equals(status)) {
                    log.info("플로우 {} v{} 상태 {} → {}", flow.flowId(), c.version(), c.status(), status);
                }
            }
            if (overlayChanged || !c.status().equals(status)) {
                reporter.report(flow.organizationId(), flow.flowId(), c.version(), flow.overlay().revision(), 0, null);
            }
            return;
        }
        long started = System.nanoTime();
        FlowCompiler.Result result = compiler.compile(flow.flowId(), flow.organizationId(), flow.activeVersion(), flow.definition());
        long compileMs = (System.nanoTime() - started) / 1_000_000;
        if (!result.ok()) {
            String error = result.errors().stream().limit(5).map(e -> e.field() + " " + e.code() + ": " + e.message())
                    .collect(Collectors.joining("; "));
            log.warn("플로우 {} v{} 컴파일 실패, 이전 버전 {} 유지: {}", flow.flowId(), flow.activeVersion(),
                    current.map(c -> "v" + c.version()).orElse("없음"), error);
            reporter.report(flow.organizationId(), flow.flowId(), current.map(LoadedFlow::version).orElse(0),
                    current.map(c -> c.overlay().revision()).orElse(0L), compileMs, error);
            return;
        }
        // ④ 원자적 전환: 이 뒤로 시작하는 메시지는 새 계획, 이전 계획을 잡은 메시지는 그 계획으로 끝까지(BR-FLW-06)
        Optional<LoadedFlow> previous = registry.put(new LoadedFlow(flow.flowId(), flow.organizationId(), flow.name(), status,
                result.plan(), flow.overlay(), flow.kind(), flow.rateLimitPerSec(), flow.pauseMode()));
        // ⑤ 상태 정리(BR-FLW-07): 삭제 노드는 24시간 보관, RESET 노드는 이전 설정의 상태·판정 타이머를 정리(읽을 때도 지문으로 걸러짐)
        StatePolicyResolver.Summary summary = StatePolicyResolver.diff(previous.map(LoadedFlow::plan).orElse(null), result.plan());
        try {
            states.retainRemoved(flow.organizationId(), flow.flowId(), result.plan().nodeIds(), clock.instant().plus(retainDeleted));
            for (String nodeId : summary.reset()) {
                int deleted = states.deleteMismatched(flow.organizationId(), flow.flowId(), nodeId,
                        result.plan().node(nodeId).stateConfig());
                int cancelled = timers.cancelNodeBefore(flow.organizationId(), flow.flowId(), nodeId, flow.activeVersion());
                log.info("플로우 {} v{} 노드 {} 상태 RESET: 상태 {}행 삭제, 판정 타이머 {}개 취소", flow.flowId(), flow.activeVersion(),
                        nodeId, deleted, cancelled);
            }
        } catch (RuntimeException e) {
            log.warn("플로우 {} 상태 정리 실패(읽을 때 지문으로 걸러지므로 동작에는 영향 없음): {}", flow.flowId(), e.getMessage());
        }
        log.info("플로우 {} v{} 적용({}ms, 노드 {}개, 상태 {}, 추가 {} 삭제 {} 변경 {}, 이전 계획 드레인 대기 {})", flow.flowId(),
                flow.activeVersion(), compileMs, result.plan().nodes().size(), status, summary.added().size(),
                summary.removed().size(), summary.changed(), registry.sweep());
        reporter.report(flow.organizationId(), flow.flowId(), flow.activeVersion(), flow.overlay().revision(), compileMs, null);
    }

    /** 적재된 플로우 대비 변경 요약(API-FLW-84 검증 응답). 적재되지 않았으면 모든 노드가 추가 */
    public StatePolicyResolver.Summary changeSummary(UUID flowId, long organizationId,
                                                     net.java21.data2flow.contracts.flow.FlowDefinition definition) {
        FlowCompiler.Result result = compiler.compile(flowId, organizationId, 0, definition);
        if (!result.ok()) {
            return null;
        }
        return StatePolicyResolver.diff(registry.get(flowId).map(LoadedFlow::plan).orElse(null), result.plan());
    }

    private void unload(UUID flowId, String reason) {
        Optional<LoadedFlow> removed = registry.remove(flowId);
        if (removed.isEmpty()) {
            return;
        }
        LoadedFlow f = removed.get();
        int cancelled = 0;
        try {
            cancelled = timers.cancelFlow(f.organizationId(), flowId);
        } catch (RuntimeException e) {
            log.warn("플로우 {} 대기 타이머 취소 실패: {}", flowId, e.getMessage());
        }
        reporter.withdraw(f.organizationId(), flowId);
        log.info("플로우 {}를 내렸습니다({}), 대기 타이머 {}개 취소", flowId, reason, cancelled);
    }

    /** 검증만(API-FLW-84): 계획을 적재하지 않는다 */
    public java.util.List<FlowValidationError> validate(UUID flowId, long organizationId, int version,
                                                         net.java21.data2flow.contracts.flow.FlowDefinition definition) {
        return compiler.compile(flowId, organizationId, version, definition).errors();
    }
}
