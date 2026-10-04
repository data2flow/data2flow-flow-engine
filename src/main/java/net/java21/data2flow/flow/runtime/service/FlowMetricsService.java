package net.java21.data2flow.flow.runtime.service;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.domain.NodeMetrics;
import net.java21.data2flow.flow.runtime.dto.FlowMetricsResponse;
import net.java21.data2flow.flow.runtime.repository.MetricRepository;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

/**
 * 플로우 지표(FLW-05.05, API-FLW-14). 원천은 {@code flow_metric_minutes}(분 단위, 인스턴스들이 더함)이다. 플로우 단위 행({@code node_id='*'})
 * 에서 실행 수·오류 실행 수·실행 시간·지연 분포·행동 수·버린 트리거를, 노드 행에서 노드별 처리·오류·평균을 읽는다. 지표 행이 아직 없는 플로우는
 * 0으로 돌려준다(core가 "지표 없음"이 아니라 0으로 그린다).
 */
public class FlowMetricsService {

    private final MetricRepository metrics;
    private final FlowRegistry registry;
    private final Clock clock;

    public FlowMetricsService(MetricRepository metrics, FlowRegistry registry, Clock clock) {
        this.metrics = metrics;
        this.registry = registry;
        this.clock = clock;
    }

    public FlowMetricsResponse metrics(UUID flowId, String window, String step) {
        Duration w = switch (window == null ? "1h" : window) {
            case "1h" -> Duration.ofHours(1);
            case "24h" -> Duration.ofHours(24);
            case "7d" -> Duration.ofDays(7);
            default -> throw new BusinessException(CommonErrorCode.INVALID_REQUEST);
        };
        Duration s = switch (step == null ? (w.compareTo(Duration.ofHours(1)) <= 0 ? "1m" : "1h") : step) {
            case "1m" -> Duration.ofMinutes(1);
            case "1h" -> Duration.ofHours(1);
            default -> throw new BusinessException(CommonErrorCode.INVALID_REQUEST);
        };
        Instant to = clock.instant().truncatedTo(ChronoUnit.MINUTES).plus(Duration.ofMinutes(1));
        Instant from = to.minus(w);
        Optional<Long> org = registry.get(flowId).map(LoadedFlow::organizationId).or(() -> metrics.findOrganization(flowId));
        List<MetricRepository.MinuteRow> rows = org.map(o -> metrics.rows(o, flowId, from, to)).orElse(List.of());

        long executions = 0;
        long errors = 0;
        long totalMs = 0;
        long command = 0;
        long notify = 0;
        long sink = 0;
        long dropped = 0;
        long[] buckets = new long[NodeMetrics.BUCKET_BOUNDS_MS.length + 1];
        Map<String, long[]> nodes = new LinkedHashMap<>();
        Map<Instant, long[]> series = new TreeMap<>();
        for (Instant t = from; t.isBefore(to); t = t.plus(s)) {
            series.put(t, new long[2]);
        }
        for (MetricRepository.MinuteRow r : rows) {
            if (NodeMetrics.FLOW_NODE.equals(r.nodeId())) {
                executions += r.processed();
                errors += r.errors();
                totalMs += r.totalMs();
                command += r.command();
                notify += r.notifyCount();
                sink += r.sink();
                dropped += r.dropped();
                for (int i = 0; i < Math.min(buckets.length, r.buckets().length); i++) {
                    buckets[i] += r.buckets()[i];
                }
                Instant slot = from.plus(s.multipliedBy(Duration.between(from, r.minute()).toMillis() / s.toMillis()));
                long[] p = series.computeIfAbsent(slot, k -> new long[2]);
                p[0] += r.processed();
                p[1] += r.errors();
            } else {
                long[] n = nodes.computeIfAbsent(r.nodeId(), k -> new long[3]);
                n[0] += r.processed();
                n[1] += r.errors();
                n[2] += r.totalMs();
            }
        }
        List<FlowMetricsResponse.Node> nodeList = new ArrayList<>();
        nodes.forEach((id, n) -> nodeList.add(new FlowMetricsResponse.Node(id, n[0], n[1], n[0] == 0 ? 0 : round((double) n[2] / n[0]))));
        List<FlowMetricsResponse.Point> points = new ArrayList<>();
        series.forEach((t, p) -> points.add(new FlowMetricsResponse.Point(t, p[0], p[1])));
        double rate = executions == 0 ? 0 : (double) errors / executions;
        return new FlowMetricsResponse(new FlowMetricsResponse.Summary(executions, errors, round(rate * 10_000) / 10_000.0,
                executions == 0 ? 0 : round((double) totalMs / executions), p95(buckets),
                new FlowMetricsResponse.Actions(command, notify, sink), dropped), nodeList, points);
    }

    /** 분포에서 p95(구간 상한). 마지막 구간(5초 초과)은 10초로 본다 */
    static double p95(long[] buckets) {
        long total = 0;
        for (long b : buckets) {
            total += b;
        }
        if (total == 0) {
            return 0;
        }
        long rank = (long) Math.ceil(total * 0.95);
        long cumulative = 0;
        for (int i = 0; i < buckets.length; i++) {
            cumulative += buckets[i];
            if (cumulative >= rank) {
                return i < NodeMetrics.BUCKET_BOUNDS_MS.length ? NodeMetrics.BUCKET_BOUNDS_MS[i] : 10_000;
            }
        }
        return 10_000;
    }

    private static double round(double v) {
        return Math.round(v * 100) / 100.0;
    }
}
