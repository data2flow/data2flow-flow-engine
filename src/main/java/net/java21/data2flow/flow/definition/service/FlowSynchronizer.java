package net.java21.data2flow.flow.definition.service;

import net.java21.data2flow.flow.apply.service.ApplyReporter;
import net.java21.data2flow.flow.common.DeploymentScope;
import net.java21.data2flow.flow.definition.domain.FlowRuntime;
import net.java21.data2flow.flow.definition.domain.RuntimeSnapshot;
import net.java21.data2flow.flow.plan.domain.FlowValidationError;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.repository.NodeStateRepository;
import net.java21.data2flow.flow.timer.repository.TimerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
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
 * 설정만 바뀐 노드의 KEEP/RESET/MIGRATE(FLW-06.03)는 M4에서 만든다(M3는 같은 노드 ID의 상태를 그대로 이어받는다).
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
    private volatile Long lastVersion;
    private volatile boolean synced;

    public FlowSynchronizer(CoreFlowDirectory core, FlowCompiler compiler, FlowRegistry registry, ApplyReporter reporter,
                            NodeStateRepository states, TimerRepository timers, DeploymentScope scope, Duration retainDeleted,
                            Clock clock) {
        this.core = core;
        this.compiler = compiler;
        this.registry = registry;
        this.reporter = reporter;
        this.states = states;
        this.timers = timers;
        this.scope = scope;
        this.retainDeleted = retainDeleted;
        this.clock = clock;
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
        Optional<LoadedFlow> current = registry.get(flow.flowId());
        if (current.isPresent() && current.get().version() == flow.activeVersion()) {
            LoadedFlow c = current.get();
            boolean overlayChanged = c.overlay().revision() != flow.overlay().revision();
            if (overlayChanged || !c.status().equals(flow.status())) {
                registry.put(new LoadedFlow(c.flowId(), c.organizationId(), flow.name(), flow.status(), c.plan(), flow.overlay()));
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
        registry.put(new LoadedFlow(flow.flowId(), flow.organizationId(), flow.name(), flow.status(), result.plan(),
                flow.overlay()));
        try {
            states.retainRemoved(flow.organizationId(), flow.flowId(), result.plan().nodeIds(), clock.instant().plus(retainDeleted));
        } catch (RuntimeException e) {
            log.warn("플로우 {} 삭제 노드 상태 보관 표시 실패(다음 적용에 다시): {}", flow.flowId(), e.getMessage());
        }
        log.info("플로우 {} v{} 적용({}ms, 노드 {}개, 상태 {})", flow.flowId(), flow.activeVersion(), compileMs,
                result.plan().nodes().size(), flow.status());
        reporter.report(flow.organizationId(), flow.flowId(), flow.activeVersion(), flow.overlay().revision(), compileMs, null);
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
