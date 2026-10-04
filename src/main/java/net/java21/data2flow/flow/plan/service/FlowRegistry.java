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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * flowId → {@code AtomicReference<LoadedFlow>}(design/flow-engine-and-live-reload.md §3 FlowRegistry). 새 버전은 완성된 계획으로
 * 참조를 한 번에 바꾸고(④ 원자적 전환), 이전 계획은 처리 중인 메시지가 끝나면 닫는다(⑥ 참조 카운트 드레인).
 *
 * <p>메시지는 처리를 시작할 때 {@link #pin}으로 그때의 적재 정보 하나를 잡는다(계획 참조 수 +1). 끝나면 {@code plan().release()}.
 * 잡는 순간 계획이 교체·드레인되어 닫혔으면 참조를 다시 읽어 새 계획을 잡으므로, 닫힌 계획으로 처리되는 메시지는 없고 한 메시지는 계획
 * 하나로만 처리된다(BR-FLW-06).
 */
public class FlowRegistry {

    private final Map<UUID, AtomicReference<LoadedFlow>> flows = new ConcurrentHashMap<>();
    private final List<ExecutionPlan> draining = new CopyOnWriteArrayList<>();

    public Optional<LoadedFlow> get(UUID flowId) {
        AtomicReference<LoadedFlow> ref = flows.get(flowId);
        return ref == null ? Optional.empty() : Optional.ofNullable(ref.get());
    }

    /**
     * 지금 적재된 플로우를 잡는다(계획 참조 수 +1). 내려간 플로우면 빈 값. 잡은 쪽은 처리가 끝나면 {@code plan().release()}를 부른다.
     */
    public Optional<LoadedFlow> pin(UUID flowId) {
        AtomicReference<LoadedFlow> ref = flows.get(flowId);
        if (ref == null) {
            return Optional.empty();
        }
        while (true) {
            LoadedFlow current = ref.get();
            if (current == null || flows.get(flowId) != ref) {
                return Optional.empty();
            }
            if (current.plan().tryAcquire()) {
                return Optional.of(current);
            }
            // 잡으려던 계획이 방금 드레인되어 닫혔다: 교체된 새 계획을 다시 읽는다
        }
    }

    /** 바꿔 끼운다. 이전 계획이 다른 계획이면 드레인 목록에 넣는다. 이전 적재 정보(없으면 빈 값) */
    public Optional<LoadedFlow> put(LoadedFlow flow) {
        LoadedFlow previous = flows.computeIfAbsent(flow.flowId(), k -> new AtomicReference<>()).getAndSet(flow);
        if (previous != null && previous.plan() != flow.plan()) {
            draining.add(previous.plan());
        }
        sweep();
        return Optional.ofNullable(previous);
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

    /** 이 조직에 적재된 플로우(일시 정지 포함: 버린 트리거를 세거나 보관하려고) */
    public List<LoadedFlow> loaded(long organizationId) {
        return all().stream().filter(f -> f.organizationId() == organizationId).toList();
    }

    /** 실행 중인 플로우 ID(타이머 폴러 범위) */
    public Set<UUID> runningIds() {
        return all().stream().filter(LoadedFlow::running).map(LoadedFlow::flowId).collect(Collectors.toUnmodifiableSet());
    }

    public Collection<UUID> ids() {
        return Set.copyOf(flows.keySet());
    }

    /** 드레인이 끝난 이전 계획을 닫고 버린다. 아직 처리 중인 이전 계획 수 */
    public int sweep() {
        draining.removeIf(ExecutionPlan::closeIfDrained);
        return draining.size();
    }

    /** 드레인 중인 이전 계획(시험·관측) */
    public List<ExecutionPlan> draining() {
        return List.copyOf(draining);
    }
}
