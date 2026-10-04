package net.java21.data2flow.flow.guard.service;

import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.message.event.EmergencyStopChanged;
import net.java21.data2flow.contracts.message.event.MaintenanceChanged;
import net.java21.data2flow.flow.definition.service.CoreFlowDirectory;
import net.java21.data2flow.flow.plan.domain.ActionDraft;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.runtime.domain.ActionGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 자동 제어 차단(BR-FLW-19, ACT-06.03 비상 정지 · OPS-05.02 유지보수 자동 제어 정지). 플로우의 제어·장면 행동(우선순위 AUTO)이 범위 안이면
 * 내보내지 않고 "skipped(EMERGENCY_STOP)"·"skipped(MAINTENANCE)"를 기록한다. action도 같은 판정을 하지만(BR-ACT-12), 엔진에서 먼저 막아
 * 아웃박스·명령 이력에 남지 않게 하고 추적에 이유가 보이게 한다.
 *
 * <p>상태 원천은 core다. 시작·{@code data2flow.config}(EMERGENCY_STOP)·주기 동기화 때 다시 읽고({@link #refresh}), EVT-ACT-03·EVT-OPS-02를
 * 받으면 바로 반영한다. 범위 판정: 조직 전체는 모두, 공간 범위는 행동 대상 공간·출처 공간·트리거 공간이 그 공간이거나 대상 기기·트리거 기기가
 * 그 공간(하위 포함, API-DEV-128)에 있으면 덮는다.
 */
public class AutomationGuard implements ActionGuard {

    private static final Logger log = LoggerFactory.getLogger(AutomationGuard.class);
    public static final String EMERGENCY_STOP = "EMERGENCY_STOP";
    public static final String MAINTENANCE = "MAINTENANCE";

    private final CoreFlowDirectory core;
    /** 비상 정지 ID → (조직, 범위, 범위 안 기기) */
    private final Map<Long, Stop> stops = new ConcurrentHashMap<>();
    /** 유지보수 창 ID → 창 */
    private final Map<Long, MaintenanceChanged> maintenance = new ConcurrentHashMap<>();

    private record Stop(long organizationId, EmergencyStopChanged.Scope scope, Set<Long> devices) {
    }

    public AutomationGuard(CoreFlowDirectory core) {
        this.core = core;
    }

    @Override
    public Optional<String> skipReason(long organizationId, ActionDraft action, FlowMessage trigger) {
        if (!action.controlsDevices() || (stops.isEmpty() && maintenance.isEmpty())) {
            return Optional.empty();
        }
        Targets t = Targets.of(action, trigger);
        for (Stop s : stops.values()) {
            if (s.organizationId() == organizationId && EmergencyStopChanged.Scope.blocks(CommandPriority.AUTO) && covers(s, t)) {
                return Optional.of(EMERGENCY_STOP);
            }
        }
        for (MaintenanceChanged m : maintenance.values()) {
            if (m.pauseAutomation() && t.anyCovered(m)) {
                return Optional.of(MAINTENANCE);
            }
        }
        return Optional.empty();
    }

    private static boolean covers(Stop s, Targets t) {
        if (s.scope().type() == EmergencyStopChanged.Scope.Type.ORG) {
            return true;
        }
        Long space = s.scope().spaceId();
        if (space == null) {
            return false;
        }
        if (t.spaces().contains(space)) {
            return true;
        }
        return !Boolean.FALSE.equals(s.scope().includeChildren()) && t.devices().stream().anyMatch(s.devices()::contains);
    }

    /** 행동 대상·출처·트리거의 기기·공간 */
    record Targets(Set<Long> devices, Set<Long> spaces) {

        static Targets of(ActionDraft action, FlowMessage trigger) {
            Set<Long> devices = new LinkedHashSet<>();
            Set<Long> spaces = new LinkedHashSet<>();
            JsonNode p = action.payload();
            JsonNode target = p == null ? null : p.path("payload").path("target");
            add(devices, target == null ? null : target.get("deviceId"));
            add(spaces, target == null ? null : target.get("spaceId"));
            add(spaces, p == null ? null : p.path("source").get("spaceId"));
            if (trigger != null) {
                add(devices, trigger.body().get("deviceId"));
                add(spaces, trigger.body().get("spaceId"));
            }
            return new Targets(devices, spaces);
        }

        private static void add(Set<Long> into, JsonNode value) {
            Double d = Jsons.number(value);
            if (d != null) {
                into.add(d.longValue());
            }
        }

        boolean anyCovered(MaintenanceChanged m) {
            for (Long d : devices) {
                if (m.covers(d, null)) {
                    return true;
                }
            }
            for (Long s : spaces) {
                if (m.covers(null, s)) {
                    return true;
                }
            }
            return false;
        }
    }

    /** EVT-ACT-03 시작 */
    public void emergencyStarted(long organizationId, EmergencyStopChanged event) {
        stops.put(event.id(), new Stop(organizationId, event.scope(), devicesOf(organizationId, event.scope())));
        log.warn("비상 정지 {} 시작(조직 {}, 범위 {}): 플로우 제어·장면을 건너뜁니다", event.id(), organizationId, event.scope());
    }

    /** EVT-ACT-03 해제 */
    public void emergencyReleased(EmergencyStopChanged event) {
        if (stops.remove(event.id()) != null) {
            log.info("비상 정지 {} 해제", event.id());
        }
    }

    /** EVT-OPS-02 시작 */
    public void maintenanceStarted(MaintenanceChanged event) {
        maintenance.put(event.windowId(), event);
    }

    /** EVT-OPS-02 종료 */
    public void maintenanceEnded(MaintenanceChanged event) {
        maintenance.remove(event.windowId());
    }

    /** core에서 다시 읽는다(시작·재연결·EMERGENCY_STOP 설정 변경·주기). 실패하면 지금 상태를 둔다 */
    public void refresh() {
        try {
            Map<Long, Stop> next = new ConcurrentHashMap<>();
            for (JsonNode s : core.activeEmergencyStops()) {
                Double id = Jsons.number(s, "emergencyStopId");
                Double org = Jsons.number(s, "organizationId");
                JsonNode scope = s.path("scope");
                if (id == null) {
                    continue;
                }
                EmergencyStopChanged.Scope sc = "SPACE".equals(scope.path("type").asString("ORG")) && Jsons.number(scope, "spaceId") != null
                        ? new EmergencyStopChanged.Scope(EmergencyStopChanged.Scope.Type.SPACE, Jsons.number(scope, "spaceId").longValue(),
                        scope.path("includeChildren").asBoolean(true))
                        : EmergencyStopChanged.Scope.organization();
                long organization = org == null ? 0 : org.longValue();
                Stop existing = stops.get(id.longValue());
                next.put(id.longValue(), existing != null && existing.scope().equals(sc) ? existing
                        : new Stop(organization, sc, devicesOf(organization, sc)));
            }
            stops.keySet().retainAll(next.keySet());
            stops.putAll(next);
        } catch (RuntimeException e) {
            log.warn("비상 정지 목록을 읽지 못했습니다(지금 상태 유지): {}", e.getMessage());
        }
        try {
            Map<Long, MaintenanceChanged> next = new ConcurrentHashMap<>();
            long synthetic = -1;
            for (JsonNode m : core.activeMaintenance()) {
                Double id = Jsons.number(m, "windowId");
                Double target = Jsons.number(m, "targetId");
                if (target == null) {
                    continue;
                }
                List<Long> descendants = new ArrayList<>();
                m.path("descendantSpaceIds").values().forEach(v -> {
                    Double d = Jsons.number(v);
                    if (d != null) {
                        descendants.add(d.longValue());
                    }
                });
                MaintenanceChanged.TargetType type = "DEVICE".equals(m.path("targetType").asString("SPACE"))
                        ? MaintenanceChanged.TargetType.DEVICE : MaintenanceChanged.TargetType.SPACE;
                long key = id == null ? synthetic-- : id.longValue();
                next.put(key, new MaintenanceChanged(key, type, target.longValue(), descendants,
                        m.path("pauseAutomation").asBoolean(false), m.path("excludeFromAnalytics").asBoolean(false),
                        java.time.Instant.EPOCH, null));
            }
            maintenance.keySet().retainAll(next.keySet());
            maintenance.putAll(next);
        } catch (RuntimeException e) {
            log.warn("유지보수 창 목록을 읽지 못했습니다(지금 상태 유지): {}", e.getMessage());
        }
    }

    private Set<Long> devicesOf(long organizationId, EmergencyStopChanged.Scope scope) {
        if (scope.type() != EmergencyStopChanged.Scope.Type.SPACE || scope.spaceId() == null) {
            return Set.of();
        }
        try {
            return core.spaceDevices(organizationId, scope.spaceId(), !Boolean.FALSE.equals(scope.includeChildren()));
        } catch (RuntimeException e) {
            log.warn("비상 정지 공간 {}의 기기를 읽지 못했습니다(공간 ID로만 판정): {}", scope.spaceId(), e.getMessage());
            return Set.of();
        }
    }

    /** 진행 중인 비상 정지 수(관측·시험) */
    public int activeStops() {
        return stops.size();
    }
}
