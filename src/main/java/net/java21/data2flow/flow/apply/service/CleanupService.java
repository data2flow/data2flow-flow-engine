package net.java21.data2flow.flow.apply.service;

import net.java21.data2flow.flow.apply.repository.InstanceVersionRepository;
import net.java21.data2flow.flow.common.DeploymentScope;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.outbox.repository.OutboxRepository;
import net.java21.data2flow.flow.runtime.repository.NodeStateRepository;
import net.java21.data2flow.flow.timer.repository.TimerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;

/**
 * 정리(ERD README §14 시스템 보관 기간): 끝난 타이머 7일, 보낸 아웃박스 7일, 삭제 노드 상태 24시간, 10분 넘게 보고 없는 인스턴스 행.
 * 하트비트: 이 인스턴스의 적용 버전 행 보고 시각을 갱신한다. 여러 인스턴스가 같이 돌려도 결과가 같다(멱등 DELETE).
 */
public class CleanupService {

    private static final Logger log = LoggerFactory.getLogger(CleanupService.class);

    private final TimerRepository timers;
    private final OutboxRepository outbox;
    private final NodeStateRepository states;
    private final InstanceVersionRepository versions;
    private final DeploymentScope scope;
    private final FlowEngineProperties.Retention retention;
    private final String instanceId;
    private final Clock clock;

    public CleanupService(TimerRepository timers, OutboxRepository outbox, NodeStateRepository states,
                          InstanceVersionRepository versions, DeploymentScope scope, FlowEngineProperties.Retention retention,
                          String instanceId, Clock clock) {
        this.timers = timers;
        this.outbox = outbox;
        this.states = states;
        this.versions = versions;
        this.scope = scope;
        this.retention = retention;
        this.instanceId = instanceId;
        this.clock = clock;
    }

    public void heartbeat() {
        Instant now = clock.instant();
        scope.organizations().forEach(org -> versions.touch(instanceId, org, now));
    }

    public void cleanup() {
        Instant now = clock.instant();
        int t = timers.deleteFinished(now.minus(retention.finishedTimers()));
        int o = outbox.deleteSent(now.minus(retention.sentOutboxes()));
        int s = states.deleteExpired(now);
        int i = versions.deleteStale(now.minus(retention.staleInstance()));
        if (t + o + s + i > 0) {
            log.info("정리: 끝난 타이머 {}, 보낸 아웃박스 {}, 보관 끝난 노드 상태 {}, 오래된 인스턴스 행 {}", t, o, s, i);
        }
    }
}
