package net.java21.data2flow.flow.liveview.service;

import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.contracts.message.FlowDebugMessage;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.runtime.domain.ExecutionListener;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 라이브 뷰 노드 카운터(FLW-03.01 실시간 흐름 표시, FLW-03.03 상태 배지, EVT-FLW-01 {@code node.stats}). 커밋된 실행에서 노드별 들어온 수·
 * 포트별 나간 수·오류·마지막 처리 시각을 1초 동안 모았다가 {@link #flush}에서 {@link FlowDebugMessage#stats}로 낸다(활동이 있던 플로우만).
 *
 * <p>상태 배지(TC-FLW-069): 최근 5분 오류 0이면 OK, 오류율 1% 이상이면 WARN, 마지막 실행이 오류였거나 오류율 10% 이상이면 ERROR와 마지막
 * 오류 문구(500자). 오류가 있었지만 1% 미만이면 OK다.
 */
public class LiveStatsCollector implements ExecutionListener {

    static final Duration HEALTH_WINDOW = Duration.ofMinutes(5);
    static final int LAST_ERROR_MAX = 500;
    private static final long BUCKET_SECONDS = 10;

    private final Clock clock;
    private final String instanceId;
    private final Map<UUID, FlowStats> flows = new ConcurrentHashMap<>();

    public LiveStatsCollector(Clock clock, String instanceId) {
        this.clock = clock;
        this.instanceId = instanceId;
    }

    @Override
    public void executed(LoadedFlow flow, ExecutionReport report) {
        Instant now = clock.instant();
        FlowStats f = flows.computeIfAbsent(flow.flowId(), k -> new FlowStats());
        synchronized (f) {
            f.version = report.version();
            f.dirty = true;
            for (ExecutionReport.Step s : report.steps()) {
                NodeStats n = f.nodes.computeIfAbsent(s.nodeId(), k -> new NodeStats());
                n.in++;
                n.lastAt = report.startedAt();
                for (String port : s.ports()) {
                    if (!"retry".equals(port)) {
                        n.out.merge(port, 1L, Long::sum);
                    }
                }
                boolean error = s.ports().contains(FlowNodeType.ERROR_PORT);
                if (error) {
                    n.errors++;
                    n.lastError = truncate(s.errorType() + ": " + s.errorMessage());
                }
                n.health.add(now, error);
            }
        }
    }

    private static String truncate(String text) {
        return text == null || text.length() <= LAST_ERROR_MAX ? text : text.substring(0, LAST_ERROR_MAX);
    }

    /**
     * 1초 동안 모은 카운터를 내보내고 비운다.
     *
     * @param sink 플로우별 디버그 메시지를 받을 곳(손실 허용 발행)
     * @return 낸 메시지 수
     */
    public int flush(Consumer<FlowDebugMessage> sink) {
        Instant now = clock.instant();
        int sent = 0;
        for (Map.Entry<UUID, FlowStats> e : flows.entrySet()) {
            FlowStats f = e.getValue();
            FlowDebugMessage message;
            synchronized (f) {
                if (!f.dirty) {
                    f.nodes.values().removeIf(n -> n.health.empty(now));
                    if (f.nodes.isEmpty()) {
                        flows.remove(e.getKey(), f);
                    }
                    continue;
                }
                List<FlowDebugMessage.NodeStats> stats = new ArrayList<>();
                f.nodes.forEach((nodeId, n) -> {
                    if (n.in > 0) {
                        stats.add(new FlowDebugMessage.NodeStats(nodeId, n.in, Map.copyOf(n.out), n.errors, n.lastAt,
                                n.status(now), n.lastError));
                    }
                    n.in = 0;
                    n.errors = 0;
                    n.out.clear();
                });
                f.dirty = false;
                message = FlowDebugMessage.stats(e.getKey().toString(), instanceId, now, f.version, stats);
            }
            sink.accept(message);
            sent++;
        }
        return sent;
    }

    /** 노드의 지금 배지(시험·조회) */
    public FlowDebugMessage.NodeStatus status(UUID flowId, String nodeId) {
        FlowStats f = flows.get(flowId);
        if (f == null) {
            return FlowDebugMessage.NodeStatus.OK;
        }
        synchronized (f) {
            NodeStats n = f.nodes.get(nodeId);
            return n == null ? FlowDebugMessage.NodeStatus.OK : n.status(clock.instant());
        }
    }

    private static final class FlowStats {
        private final Map<String, NodeStats> nodes = new LinkedHashMap<>();
        private int version;
        private boolean dirty;
    }

    private static final class NodeStats {
        private long in;
        private long errors;
        private final Map<String, Long> out = new LinkedHashMap<>();
        private Instant lastAt;
        private String lastError;
        private final Health health = new Health();

        FlowDebugMessage.NodeStatus status(Instant now) {
            long[] t = health.totals(now);
            if (t[0] == 0 || t[1] == 0) {
                return FlowDebugMessage.NodeStatus.OK;
            }
            double rate = (double) t[1] / t[0];
            if (health.lastWasError || rate >= 0.10) {
                return FlowDebugMessage.NodeStatus.ERROR;
            }
            return rate >= 0.01 ? FlowDebugMessage.NodeStatus.WARN : FlowDebugMessage.NodeStatus.OK;
        }
    }

    /** 노드 하나의 최근 5분 처리·오류 수(10초 단위) */
    static final class Health {
        private final Deque<long[]> buckets = new ArrayDeque<>();
        private boolean lastWasError;

        void add(Instant at, boolean error) {
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
            lastWasError = error;
            long oldest = slot - HEALTH_WINDOW.toSeconds() / BUCKET_SECONDS;
            while (!buckets.isEmpty() && buckets.peekFirst()[0] <= oldest) {
                buckets.pollFirst();
            }
        }

        long[] totals(Instant now) {
            long oldest = now.getEpochSecond() / BUCKET_SECONDS - HEALTH_WINDOW.toSeconds() / BUCKET_SECONDS;
            long n = 0;
            long e = 0;
            for (long[] b : buckets) {
                if (b[0] > oldest) {
                    n += b[1];
                    e += b[2];
                }
            }
            return new long[]{n, e};
        }

        boolean empty(Instant now) {
            return totals(now)[0] == 0;
        }
    }
}
