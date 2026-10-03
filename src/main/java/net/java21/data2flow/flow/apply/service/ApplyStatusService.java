package net.java21.data2flow.flow.apply.service;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.flow.apply.dto.ApplyStatusResponse;
import net.java21.data2flow.flow.apply.repository.InstanceVersionRepository;

import java.time.Clock;
import java.time.Duration;
import java.util.UUID;

/** API-FLW-82 적용 상태 조회 */
public class ApplyStatusService {

    private final InstanceVersionRepository versions;
    private final Duration staleAfter;
    private final Clock clock;

    public ApplyStatusService(InstanceVersionRepository versions, Duration staleAfter, Clock clock) {
        this.versions = versions;
        this.staleAfter = staleAfter;
        this.clock = clock;
    }

    public ApplyStatusResponse status(UUID flowId) {
        long organizationId = versions.findOrganization(flowId)
                .orElseThrow(() -> new BusinessException(CommonErrorCode.RESOURCE_NOT_FOUND));
        var instances = versions.findByFlow(organizationId, flowId, clock.instant().minus(staleAfter)).stream()
                .map(v -> new ApplyStatusResponse.Instance(v.instanceId(), v.appliedVersion(), v.overlayRevision(), v.reportedAt()))
                .toList();
        return new ApplyStatusResponse(flowId.toString(), instances);
    }
}
