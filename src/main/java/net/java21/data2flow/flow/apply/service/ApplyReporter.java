package net.java21.data2flow.flow.apply.service;

import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.FlowApplyReported;
import net.java21.data2flow.flow.apply.repository.InstanceVersionRepository;
import net.java21.data2flow.flow.messaging.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.UUID;

/**
 * 적용 결과 보고(design/flow-engine-and-live-reload.md §4 ⑦, EVT-FLW-02): {@code flow_instance_versions}를 먼저 갱신하고
 * {@code flow.apply.reported}를 낸다. 실패(컴파일 오류)도 보고하며 그때 {@code appliedVersion}은 유지 중인 이전 버전(없으면 0)이다.
 * 이벤트 발행은 손실을 견딘다(표가 원천이고 core는 API-FLW-82로 다시 읽을 수 있다).
 */
public class ApplyReporter {

    private static final Logger log = LoggerFactory.getLogger(ApplyReporter.class);

    private final InstanceVersionRepository versions;
    private final EventPublisher events;
    private final String instanceId;
    private final Clock clock;

    public ApplyReporter(InstanceVersionRepository versions, EventPublisher events, String instanceId, Clock clock) {
        this.versions = versions;
        this.events = events;
        this.instanceId = instanceId;
        this.clock = clock;
    }

    public void report(long organizationId, UUID flowId, int appliedVersion, long overlayRevision, long compileMs, String error) {
        try {
            versions.upsert(organizationId, instanceId, flowId, appliedVersion, overlayRevision, clock.instant());
        } catch (RuntimeException e) {
            log.warn("적용 버전 기록 실패(다음 동기화에 다시): {}", e.getMessage());
        }
        try {
            events.publish(EventType.FLOW_APPLY_REPORTED, organizationId,
                    new FlowApplyReported(flowId.toString(), instanceId, appliedVersion, overlayRevision, compileMs, error));
        } catch (RuntimeException e) {
            log.warn("flow.apply.reported 발행 실패(손실 허용): {}", e.getMessage());
        }
    }

    public void withdraw(long organizationId, UUID flowId) {
        try {
            versions.delete(organizationId, instanceId, flowId);
        } catch (RuntimeException e) {
            log.warn("적용 버전 행 삭제 실패: {}", e.getMessage());
        }
    }

    public String instanceId() {
        return instanceId;
    }
}
