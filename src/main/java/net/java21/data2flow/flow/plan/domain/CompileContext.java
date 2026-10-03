package net.java21.data2flow.flow.plan.domain;

import java.util.UUID;

/**
 * 컴파일 중인 플로우 정보.
 *
 * @param flowId         플로우 ID
 * @param organizationId 조직
 * @param version        컴파일하는 버전
 */
public record CompileContext(UUID flowId, long organizationId, int version) {
}
