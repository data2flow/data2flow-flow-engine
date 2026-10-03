package net.java21.data2flow.flow.plan.service;

import net.java21.data2flow.flow.plan.domain.ExecutionPlan;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * flowId → {@code AtomicReference<LoadedFlow>}(design/flow-engine-and-live-reload.md §3 FlowRegistry). 새 버전은 완성된 계획으로
 * 참조를 한 번에 바꾸고(④ 원자적 전환), 이전 계획은 처리 중인 메시지가 끝나면({@link ExecutionPlan#drained()}) 버린다(⑥ 드레인).
 * 메시지는 처리를 시작할 때 {@link #get}으로 한 번 읽은 계획만 쓴다(BR-FLW-06).
 */
public class FlowRegistry {

    private final Map<UUID, AtomicReference<LoadedFlow>> flows = new ConcurrentHashMap<>();
    private final List<ExecutionPlan> draining = new java.util.concurrent.CopyOnWriteArrayList<>();

    public Optional<LoadedFlow> get(UUID flowId) {
        AtomicReference<LoadedFlow> ref = flows.get(flowId);
        return ref == null ? Optional.empty() : Optional.ofNullable(ref.get());
    }

    /** 바꿔 끼운다. 이전 계획이 다른 버전이면 드레인 목록에 넣는다 */
    public void put(LoadedFlow flow) {
        LoadedFlow previous = flows.computeIfAbsent(flow.flowId(), k -> new AtomicReference<>()).getAndSet(flow);
        if (previous != null && previous.plan() != flow.plan()) {
            draining.add(previous.plan());
        }
        sweep();
    }

    public Optional<LoadedFlow> remove(UUID flowId) {
        AtomicReference<LoadedFlow> ref = flows.remove(flowId);
        LoadedFlow previous = ref == null ? null : ref.get();
        if (previous != null) {
            draining.add(previous.plan());
        }
        sweep();
        return Optional.ofNullable(previous);
    }

    public List<LoadedFlow> all() {
        List<LoadedFlow> out = new ArrayList<>();
        flows.values().forEach(r -> {
            LoadedFlow f = r.get();
            if (f != null) {
                out.add(f);
            }
        });
        return out;
    }

    /** 이 조직에서 트리거를 받는 플로우(TriggerIndex, 조직당 활성 플로우 500개 한도라 순회로 충분) */
    public List<LoadedFlow> running(long organizationId) {
        return all().stream().filter(f -> f.organizationId() == organizationId && f.running()).toList();
    }

    /** 실행 중인 플로우 ID(타이머 폴러 범위) */
    public Set<UUID> runningIds() {
        return all().stream().filter(LoadedFlow::running).map(LoadedFlow::flowId).collect(Collectors.toUnmodifiableSet());
    }

    public Collection<UUID> ids() {
        return Set.copyOf(flows.keySet());
    }

    /** 드레인이 끝난 이전 계획을 버린다. 남아 있는 수 */
    public int sweep() {
        draining.removeIf(ExecutionPlan::drained);
        return draining.size();
    }
}
