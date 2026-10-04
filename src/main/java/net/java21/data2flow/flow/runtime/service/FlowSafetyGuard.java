package net.java21.data2flow.flow.runtime.service;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.event.FlowStateChanged;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.domain.ExecutionListener;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.runtime.domain.FlowStateNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 플로우 안전장치(FLW-05.04, BR-FLW-14)와 오류율 감시(FLW-08.03, BR-FLW-26).
 *
 * <ul>
 *   <li><b>초당 실행 한도</b>: 플로우마다 초당 실행 수가 {@code rateLimitPerSec}(기본 100, 최대 1,000, 버전에 포함)를 넘으면 폭주(RUNAWAY)로
 *       보고 이 인스턴스에서 바로 멈추고(PAUSED) EVT-FLW-03을 낸다. core-api가 상태를 PAUSED로 저장하고 MAJOR 알람을 만든다. 한도는 인스턴스마다
 *       센다(한 플로우의 트리거가 여러 파티션에 나뉘면 인스턴스별 한도).</li>
 *   <li><b>순환</b>: 텔레메트리의 메시지 계보({@code meta.lineage}: 이 메시지를 만든 플로우 ID 목록)에 같은 플로우가 있으면, 즉 플로우 결과가 다시
 *       자기 트리거를 일으켰으면(다른 플로우를 거친 간접 순환 A→B→A 포함) 순환(CYCLE)으로 멈춘다. 파생 텔레메트리를 만드는 생산자는 계보를
 *       이어 붙여야 한다.</li>
 *   <li><b>엔진이 멈춘 플로우</b>는 core가 PAUSED를 돌려줄 때까지(또는 새 버전이 적용될 때까지) 주기 동기화가 다시 ACTIVE로 되돌리지 않는다.</li>
 *   <li><b>DEGRADED</b>: 최근 5분 실행 100건 이상이고 오류율이 기준(기본 10%) 이상이면 DEGRADED, 그 뒤 10분 연속 기준 미만이면 RECOVERED.</li>
 * </ul>
 */
public class FlowSafetyGuard implements ExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(FlowSafetyGuard.class);
    private static final long BUCKET_SECONDS = 10;
    private static final Duration WINDOW = Duration.ofMinutes(5);
    private static final Duration RECOVERY = Duration.ofMinutes(10);
    static final int MIN_EXECUTIONS = 100;

    private final FlowRegistry registry;
    private final FlowStateNotifier notifier;
    private final Clock clock;
    private final double errorRateThreshold;
    private final Map<UUID, long[]> rates = new ConcurrentHashMap<>();
    private final Map<UUID, Integer> autoPaused = new ConcurrentHashMap<>();
    private final Map<UUID, Health> health = new ConcurrentHashMap<>();

    public FlowSafetyGuard(FlowRegistry registry, FlowStateNotifier notifier, Clock clock, double errorRateThreshold) {
        this.registry = registry;
        this.notifier = notifier;
        this.clock = clock;
        this.errorRateThreshold = errorRateThreshold;
    }

    /** 실행 하나를 받아도 되는가(초당 한도). 넘으면 멈추고 false */
    public boolean admit(LoadedFlow flow) {
        long second = clock.millis() / 1000;
        long[] w = rates.computeIfAbsent(flow.flowId(), k -> new long[]{second, 0});
        long count;
        synchronized (w) {
            if (w[0] != second) {
                w[0] = second;
                w[1] = 0;
            }
            count = ++w[1];
        }
        if (count <= flow.rateLimitPerSec()) {
            return true;
        }
        pause(flow, FlowStateChanged.Reason.RUNAWAY, count, "초당 실행 " + count + "건이 한도 " + flow.rateLimitPerSec() + "건을 넘었습니다");
        return false;
    }

    /** 순환이면 멈추고 true */
    public boolean cycle(LoadedFlow flow, CanonicalTelemetry telemetry) {
        if (!lineageContains(telemetry, flow.flowId())) {
            return false;
        }
        pause(flow, FlowStateChanged.Reason.CYCLE, 0, "메시지 계보에 같은 플로우가 다시 나타났습니다(순환)");
        return true;
    }

    /** 텔레메트리 계보({@code meta.lineage}: 플로우 ID 문자열 또는 {@code {flowId}} 목록)에 이 플로우가 있는가 */
    public static boolean lineageContains(CanonicalTelemetry telemetry, UUID flowId) {
        if (telemetry.meta() == null) {
            return false;
        }
        JsonNode lineage = telemetry.meta().extra().get("lineage");
        if (lineage == null || !lineage.isArray()) {
            return false;
        }
        String id = flowId.toString();
        for (JsonNode item : lineage.values()) {
            String f = item.isString() ? item.stringValue() : item.path("flowId").asString(null);
            if (id.equals(f)) {
                return true;
            }
        }
        return false;
    }

    private void pause(LoadedFlow flow, FlowStateChanged.Reason reason, long ratePerSec, String why) {
        LoadedFlow current = registry.get(flow.flowId()).orElse(flow);
        // 파티션 작업자 여럿이 같은 플로우를 동시에 넘겨도 한 번만 멈추고 한 번만 알린다
        if (!current.running() || autoPaused.putIfAbsent(flow.flowId(), current.version()) != null) {
            return;   // 이미 멈춤
        }
        registry.put(current.withStatus("PAUSED"));
        log.warn("플로우 {} v{}를 멈췄습니다({}): {}", flow.flowId(), current.version(), reason, why);
        notifyChange(flow.organizationId(), new FlowStateChanged(flow.flowId().toString(), current.status(), "PAUSED", reason,
                new FlowStateChanged.Metrics(errorRate(flow.flowId()), ratePerSec), clock.instant()));
    }

    /**
     * 동기화가 core 상태를 적용하기 전에 묻는다: 엔진이 멈춘 플로우를 core가 아직 모르면(같은 버전, 실행 상태) 멈춘 채로 둔다. core가
     * PAUSED를 알려 주었거나 다른 버전이 적용되었으면 엔진의 판단을 내려놓는다.
     */
    public boolean holdsPause(UUID flowId, String coreStatus, int coreVersion) {
        Integer version = autoPaused.get(flowId);
        if (version == null) {
            return false;
        }
        if ("PAUSED".equals(coreStatus) || version != coreVersion) {
            autoPaused.remove(flowId);
            return false;
        }
        return true;
    }

    @Override
    public void executed(LoadedFlow flow, ExecutionReport report) {
        health.computeIfAbsent(flow.flowId(), k -> new Health(flow.organizationId())).add(clock.instant(), report.failed());
    }

    /** 오류율 판정(1초마다, BR-FLW-26) */
    public void evaluate() {
        Instant now = clock.instant();
        for (Map.Entry<UUID, Health> e : health.entrySet()) {
            Health h = e.getValue();
            long[] totals = h.totals(now, WINDOW);
            double rate = totals[0] == 0 ? 0 : (double) totals[1] / totals[0];
            boolean over = totals[0] >= MIN_EXECUTIONS && rate >= errorRateThreshold;
            String status = registry.get(e.getKey()).map(LoadedFlow::status).orElse("ACTIVE");
            synchronized (h) {
                if (!h.degraded && over) {
                    h.degraded = true;
                    h.belowSince = null;
                    log.warn("플로우 {} 오류율 {}%(최근 5분 {}건) → DEGRADED", e.getKey(), Math.round(rate * 1000) / 10.0, totals[0]);
                    notifyChange(h.organizationId, new FlowStateChanged(e.getKey().toString(), status, "DEGRADED",
                            FlowStateChanged.Reason.DEGRADED, new FlowStateChanged.Metrics(rate, totals[0] / 300.0), now));
                } else if (h.degraded && !over) {
                    if (h.belowSince == null) {
                        h.belowSince = now;
                    } else if (!now.isBefore(h.belowSince.plus(RECOVERY))) {
                        h.degraded = false;
                        h.belowSince = null;
                        notifyChange(h.organizationId, new FlowStateChanged(e.getKey().toString(), "DEGRADED", "ACTIVE",
                                FlowStateChanged.Reason.RECOVERED, new FlowStateChanged.Metrics(rate, totals[0] / 300.0), now));
                    }
                } else if (h.degraded) {
                    h.belowSince = null;
                }
            }
            if (!h.degraded && totals[0] == 0 && h.empty(now)) {
                health.remove(e.getKey(), h);
            }
        }
    }

    /** 최근 5분 오류율(0~1) */
    public double errorRate(UUID flowId) {
        Health h = health.get(flowId);
        if (h == null) {
            return 0;
        }
        long[] t = h.totals(clock.instant(), WINDOW);
        return t[0] == 0 ? 0 : (double) t[1] / t[0];
    }

    public boolean degraded(UUID flowId) {
        Health h = health.get(flowId);
        return h != null && h.degraded;
    }

    private void notifyChange(long organizationId, FlowStateChanged change) {
        try {
            notifier.notify(organizationId, change);
        } catch (RuntimeException e) {
            log.warn("flow.state.changed 발행 실패: {}", e.getMessage());
        }
    }

    /** 10초 단위 실행·오류 수(최근 10분) */
    private static final class Health {
        private final long organizationId;
        private final Deque<long[]> buckets = new ArrayDeque<>();
        private boolean degraded;
        private Instant belowSince;

        Health(long organizationId) {
            this.organizationId = organizationId;
        }

        synchronized void add(Instant at, boolean error) {
            long slot = at.getEpochSecond() / BUCKET_SECONDS;
            long[] last = buckets.peekLast();
            if (last == null || last[0] != slot) {
                last = new long[]{slot, 0, 0};
                buckets.addLast(last);
            }
            last[1]++;
            if (error) {
                last[2]++;
            }
            long oldest = slot - RECOVERY.toSeconds() / BUCKET_SECONDS;
            while (!buckets.isEmpty() && buckets.peekFirst()[0] < oldest) {
                buckets.pollFirst();
            }
        }

        synchronized long[] totals(Instant now, Duration window) {
            long from = (now.getEpochSecond() - window.toSeconds()) / BUCKET_SECONDS;
            long exec = 0;
            long err = 0;
            for (long[] b : buckets) {
                if (b[0] > from) {
                    exec += b[1];
                    err += b[2];
                }
            }
            return new long[]{exec, err};
        }

        synchronized boolean empty(Instant now) {
            long oldest = now.getEpochSecond() / BUCKET_SECONDS - RECOVERY.toSeconds() / BUCKET_SECONDS;
            return buckets.isEmpty() || buckets.peekLast()[0] < oldest;
        }
    }
}
