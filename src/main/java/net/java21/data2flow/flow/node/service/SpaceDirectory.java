package net.java21.data2flow.flow.node.service;

import java.util.Set;

/** 공간을 측정하는 기기 목록(core API-DEV-128 {@code GET /internal/core/spaces/{space-id}/devices?relation=measures}) */
public interface SpaceDirectory {

    /** 캐시에 있으면 바로, 없으면 빈 집합을 돌려주고 뒤에서 읽어 둔다(메시지 처리를 막지 않음) */
    Set<Long> measuringDevices(long organizationId, long spaceId, boolean includeDescendants);

    /** 컴파일할 때 미리 읽어 둔다(실패해도 메시지 처리는 막지 않음) */
    default void warm(long organizationId, long spaceId, boolean includeDescendants) {
        measuringDevices(organizationId, spaceId, includeDescendants);
    }

    /**
     * 기기 태그(트리거 태그 대상, FLW-02 {@code target.tags}). 캐시에 있으면 바로, 없으면 빈 집합을 돌려주고 뒤에서 읽어 둔다(메시지 처리를
     * 막지 않음). 원천은 core API-DEV-122 {@code tags[]}.
     */
    default Set<String> deviceTags(long organizationId, long deviceId) {
        return Set.of();
    }

    /** 기기·공간 변경(EVT-DEV-04)을 받으면 다시 읽게 한다 */
    default void invalidateAll() {
        // 캐시 없음
    }

    /** 아무것도 모르는 디렉터리(시험·드라이런) */
    SpaceDirectory NONE = (organizationId, spaceId, includeDescendants) -> Set.of();
}
