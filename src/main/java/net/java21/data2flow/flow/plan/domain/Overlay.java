package net.java21.data2flow.flow.plan.domain;

import java.util.Set;

/**
 * 버전을 올리지 않는 실행 옵션(FLW-06.04, BR-FLW-10): 바이패스·디버그 노드. 실행 계획과 따로 바꿔 끼운다.
 *
 * @param bypass   바이패스한 노드 ID
 * @param debug    전체 디버그(초당 50건) 노드 ID
 * @param revision {@code flow_overlays.revision}
 */
public record Overlay(Set<String> bypass, Set<String> debug, long revision) {

    public static final Overlay NONE = new Overlay(Set.of(), Set.of(), 0);

    public Overlay {
        bypass = bypass == null ? Set.of() : Set.copyOf(bypass);
        debug = debug == null ? Set.of() : Set.copyOf(debug);
    }
}
