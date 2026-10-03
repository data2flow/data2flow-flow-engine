package net.java21.data2flow.flow.definition.service;

import net.java21.data2flow.flow.definition.domain.FlowRuntime;
import net.java21.data2flow.flow.definition.domain.RuntimeSnapshot;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** core-api의 플로우·공간 기준 정보(내부 API). 시험은 대역을 쓴다 */
public interface CoreFlowDirectory {

    /** API-FLW-80. {@code sinceVersion}과 같으면(바뀐 것 없음) 빈 값 */
    Optional<RuntimeSnapshot> runtime(Long sinceVersion);

    /** API-FLW-81. 없거나 실행 대상이 아니면 빈 값 */
    Optional<FlowRuntime> flow(UUID flowId);

    /** API-DEV-128: 공간을 측정하는 기기 ID */
    Set<Long> measuringDevices(long organizationId, long spaceId, boolean includeDescendants);
}
