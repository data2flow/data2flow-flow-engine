package net.java21.data2flow.flow.runtime.domain;

import tools.jackson.databind.JsonNode;

/**
 * 저장된 노드 상태 한 행.
 *
 * @param state       상태. 비어 있으면 null
 * @param stateConfig 상태를 쓴 노드의 상태 지문(FLW-06.03). M3 행처럼 없으면 null(지금 설정으로 쓴 것으로 본다)
 */
public record StoredState(JsonNode state, JsonNode stateConfig) {

    public static final StoredState EMPTY = new StoredState(null, null);
}
