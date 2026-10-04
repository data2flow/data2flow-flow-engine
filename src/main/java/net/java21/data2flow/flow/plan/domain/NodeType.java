package net.java21.data2flow.flow.plan.domain;

import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.contracts.flow.StatePolicy;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 노드 종류 SPI(design/flow-engine-and-live-reload.md §2.2). 새 노드 종류는 이 인터페이스 구현 하나와 카탈로그 항목
 * ({@code classpath:node-types/{type}.json}) 하나로 추가한다. 카탈로그는 API-FLW-83으로 core-api에 나가 API-FLW-30이 된다.
 *
 * <p>상태 이어받기(FLW-06.03, BR-FLW-07): 카탈로그의 {@code statePolicy}가 설정 필드마다 KEEP/RESET/MIGRATE를 정한다.
 * {@link #statePolicy(JsonNode, JsonNode)}는 바뀐 필드의 정책 중 가장 보수적인 것(RESET &gt; MIGRATE &gt; KEEP)이고, 카탈로그에 없는 필드가
 * 바뀌면 RESET이다. MIGRATE 노드는 {@link #migrateState}로 상태를 새 설정에 맞게 바꾼다.
 */
public interface NodeType {

    /** 상태 지문의 노드 종류 키 */
    String TYPE_KEY = "@type";

    /** 카탈로그 항목(설정 스키마·포트·상태 정책·권한·기본값) */
    FlowNodeType descriptor();

    default String type() {
        return descriptor().type();
    }

    /**
     * 노드 정의를 실행 가능한 노드로 바꾼다(설정 검증·사전 계산). 설정이 틀리면 {@link NodeConfigException}.
     * 결과는 불변이어야 한다(실행 계획을 여러 스레드가 함께 쓴다).
     */
    CompiledNode compile(FlowNode node, CompileContext context);

    /**
     * 설정이 바뀐 노드의 상태 정책. 두 지문({@link #stateConfig}) 또는 두 설정을 받아, 값이 다른 필드의 정책 중 가장 보수적인 것을 고른다.
     * 노드 종류가 다르면 RESET.
     */
    default StatePolicy statePolicy(JsonNode oldConfig, JsonNode newConfig) {
        if (oldConfig == null || newConfig == null) {
            return StatePolicy.KEEP;
        }
        JsonNode oldType = oldConfig.get(TYPE_KEY);
        JsonNode newType = newConfig.get(TYPE_KEY);
        if (oldType != null && newType != null && !oldType.equals(newType)) {
            return StatePolicy.RESET;
        }
        Set<String> fields = new LinkedHashSet<>();
        oldConfig.propertyNames().forEach(fields::add);
        newConfig.propertyNames().forEach(fields::add);
        fields.remove(TYPE_KEY);
        StatePolicy result = StatePolicy.KEEP;
        for (String field : fields) {
            JsonNode a = oldConfig.get(field);
            JsonNode b = newConfig.get(field);
            if (a == null ? b == null || b.isNull() : a.equals(b) || (a.isNull() && b == null)) {
                continue;
            }
            result = StatePolicy.strictest(result, fieldPolicy(field));
        }
        return result;
    }

    /** 카탈로그의 필드 정책. 선언되지 않은 필드는 RESET(안전) */
    default StatePolicy fieldPolicy(String field) {
        JsonNode p = descriptor().statePolicy();
        JsonNode v = p == null ? null : p.get(field);
        if (v == null || !v.isString()) {
            return StatePolicy.RESET;
        }
        try {
            return StatePolicy.valueOf(v.stringValue()).effective();
        } catch (IllegalArgumentException e) {
            return StatePolicy.RESET;
        }
    }

    /**
     * 상태 지문: {@code {"@type": 종류, 필드: 값…}}(정책이 KEEP이 아닌 필드만). 상태 행에 저장하고, 읽을 때 지금 노드의 지문과 비교한다.
     * KEEP 필드만 바뀐 새 버전은 지문이 같으므로 상태를 그대로 쓴다(진행 중인 지속 타이머 유지).
     */
    default ObjectNode stateConfig(FlowNode node) {
        ObjectNode out = Jsons.object();
        out.put(TYPE_KEY, type());
        JsonNode config = node.config();
        if (config != null && config.isObject()) {
            for (Map.Entry<String, JsonNode> e : config.properties()) {
                if (fieldPolicy(e.getKey()) != StatePolicy.KEEP && !e.getValue().isNull()) {
                    out.set(e.getKey(), e.getValue());
                }
            }
        }
        return out;
    }

    /**
     * MIGRATE: 이전 지문의 설정으로 쓴 상태를 새 설정에 맞게 바꾼다. 기본은 그대로 쓴다. null을 돌려주면 빈 상태.
     *
     * @param oldConfig 상태를 쓴 버전의 지문
     * @param newConfig 지금 노드의 지문
     * @param state     상태
     */
    default JsonNode migrateState(JsonNode oldConfig, JsonNode newConfig, JsonNode state) {
        return state;
    }
}
