package net.java21.data2flow.flow.runtime.service;

import net.java21.data2flow.flow.runtime.domain.NodeMetrics;
import net.java21.data2flow.flow.runtime.repository.MetricRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 노드 지표 버퍼: 실행마다 메모리에 더하고 주기적으로(기본 5초) {@code flow_metric_minutes}에 더한다. 지표는 노드 상태·아웃박스와 달리
 * 한 트랜잭션일 필요가 없고(BR-FLW-29 대상 아님), 메시지마다 같은 (플로우, 노드, 분) 행을 갱신하면 파티션끼리 행 잠금을 다퉈 처리 지연이
 * 커지기 때문이다(TC-FLW-088). 종료할 때 남은 것을 마저 쓴다. 인스턴스가 죽으면 마지막 몇 초 지표는 잃을 수 있다(관측용).
 */
public class MetricBuffer {

    private static final Logger log = LoggerFactory.getLogger(MetricBuffer.class);

    private record Key(long organizationId, UUID flowId, String nodeId, Instant minute) {
    }

    private final MetricRepository repository;
    private final Map<Key, NodeMetrics> pending = new ConcurrentHashMap<>();

    public MetricBuffer(MetricRepository repository) {
        this.repository = repository;
    }

    public void add(long organizationId, UUID flowId, String nodeId, Instant minute, NodeMetrics delta) {
        pending.merge(new Key(organizationId, flowId, nodeId, minute), delta, (a, b) -> {
            synchronized (a) {
                a.addAll(b);
            }
            return a;
        });
    }

    /** 쌓인 지표를 쓴다. 실패하면 다시 넣어 다음에 쓴다 */
    public void flush() {
        for (Key key : pending.keySet()) {
            NodeMetrics m = pending.remove(key);
            if (m == null) {
                continue;
            }
            try {
                synchronized (m) {
                    repository.add(key.organizationId(), key.flowId(), key.nodeId(), key.minute(), m);
                }
            } catch (RuntimeException e) {
                add(key.organizationId(), key.flowId(), key.nodeId(), key.minute(), m);
                log.debug("지표 기록 실패(다음에 다시): {}", e.getMessage());
                return;
            }
        }
    }
}
