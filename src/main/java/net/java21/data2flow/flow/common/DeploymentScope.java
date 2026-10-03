package net.java21.data2flow.flow.common;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 이 배포가 맡는 조직(ADR-030: staging·prod가 DB 하나를 함께 쓰고 staging은 전용 조직). core API-FLW-80이 알려 준 배포 조직과 적재한
 * 플로우의 조직을 모은다. 타이머 폴러·아웃박스 릴레이는 이 조직의 행만 가져가므로 staging 파드가 prod 조직의 타이머를 발화하거나 prod
 * 행동을 staging vhost로 보내지 않는다. 아직 모르면 비어 있고 아무 행도 가져가지 않는다.
 */
public class DeploymentScope {

    private final Set<Long> organizations = ConcurrentHashMap.newKeySet();

    public void add(long organizationId) {
        if (organizationId > 0) {
            organizations.add(organizationId);
        }
    }

    public Set<Long> organizations() {
        return Set.copyOf(organizations);
    }
}
