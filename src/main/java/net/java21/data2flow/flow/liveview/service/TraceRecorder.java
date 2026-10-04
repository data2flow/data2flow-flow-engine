package net.java21.data2flow.flow.liveview.service;

import net.java21.data2flow.flow.liveview.repository.TraceRepository;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.runtime.domain.ExecutionListener;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 실행 추적 보관(FLW-03.04): 디버그를 켠 노드를 지났거나(overlay.debug·debug.log) 샘플된(플로우당 초당 {@value #SAMPLES_PER_SECOND}건)
 * 실행만 1시간 보관한다. 커밋 뒤 대기열에 넣고 주기 작업({@link #flush})이 모아 쓴다(처리 지연에 영향 없음, 손실 허용: 인스턴스가 죽으면
 * 쓰지 못한 추적은 잃는다).
 */
public class TraceRecorder implements ExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(TraceRecorder.class);
    public static final Duration RETENTION = Duration.ofHours(1);
    static final int SAMPLES_PER_SECOND = 5;
    static final int MAX_QUEUE = 10_000;
    static final int MAX_TRACE_BYTES = 256 * 1024;

    private final TraceRepository repository;
    private final Clock clock;
    private final LinkedBlockingQueue<TraceRepository.Row> queue = new LinkedBlockingQueue<>(MAX_QUEUE);
    private final Map<UUID, long[]> windows = new ConcurrentHashMap<>();

    public TraceRecorder(TraceRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    @Override
    public void executed(LoadedFlow flow, ExecutionReport report) {
        if (!report.debugged() && !sampled(flow.flowId())) {
            return;
        }
        String json = Jsons.MAPPER.writeValueAsString(Traces.of(report));
        if (json.length() > MAX_TRACE_BYTES) {
            return;   // 너무 큰 추적은 보관하지 않는다(디버그 패널 샘플로 대신)
        }
        if (!queue.offer(new TraceRepository.Row(flow.organizationId(), flow.flowId(), report.triggerMessageId(), report.version(),
                json, clock.instant().plus(RETENTION)))) {
            log.debug("추적 대기열이 가득 차 버립니다: {}", report.triggerMessageId());
        }
    }

    /** 플로우당 초당 샘플 상한 안인가 */
    boolean sampled(UUID flowId) {
        long second = clock.millis() / 1000;
        long[] w = windows.computeIfAbsent(flowId, k -> new long[]{second, 0});
        synchronized (w) {
            if (w[0] != second) {
                w[0] = second;
                w[1] = 0;
            }
            return ++w[1] <= SAMPLES_PER_SECOND;
        }
    }

    /** 모인 추적을 쓴다(주기 작업). 쓴 수 */
    public int flush() {
        List<TraceRepository.Row> rows = new ArrayList<>();
        queue.drainTo(rows, 500);
        if (rows.isEmpty()) {
            return 0;
        }
        try {
            repository.insertAll(rows, clock.instant());
        } catch (RuntimeException e) {
            log.debug("추적 기록 실패(손실 허용): {}", e.getMessage());
            return 0;
        }
        return rows.size();
    }

    /** 1시간 지난 추적 정리 */
    public int cleanup() {
        return repository.deleteExpired(clock.instant());
    }

    public Instant now() {
        return clock.instant();
    }
}
