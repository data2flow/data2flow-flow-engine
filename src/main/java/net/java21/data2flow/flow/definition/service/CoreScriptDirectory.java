package net.java21.data2flow.flow.definition.service;

import net.java21.data2flow.flow.node.service.ScriptDirectory;
import net.java21.data2flow.flow.plan.domain.Jsons;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * core API-SCR-32 실행 묶음({@code GET /internal/core/scripts/runtime-bundle?organizationId=})으로 스크립트 참조를 푼다. 컴파일할 때만 부르므로
 * 조직마다 짧게(30초) 캐시한다. 참조한 버전이 활성 버전이 아니면 찾지 못한 것으로 본다(묶음에는 활성 버전만 있음).
 */
public class CoreScriptDirectory implements ScriptDirectory {

    private record Entry(List<JsonNode> scripts, Instant loadedAt) {
    }

    private final CoreFlowDirectory core;
    private final Duration ttl;
    private final Clock clock;
    private final Map<Long, Entry> cache = new ConcurrentHashMap<>();

    public CoreScriptDirectory(CoreFlowDirectory core, Duration ttl, Clock clock) {
        this.core = core;
        this.ttl = ttl;
        this.clock = clock;
    }

    @Override
    public Optional<String> code(long organizationId, long scriptId, int versionNo) {
        Entry e = cache.get(organizationId);
        if (e == null || e.loadedAt().plus(ttl).isBefore(clock.instant())) {
            e = new Entry(core.scriptBundle(organizationId), clock.instant());
            cache.put(organizationId, e);
        }
        for (JsonNode s : e.scripts()) {
            Double id = Jsons.number(s, "scriptId");
            Double version = Jsons.number(s, "versionNo");
            if (id != null && id.longValue() == scriptId && version != null && version.intValue() == versionNo) {
                return Optional.ofNullable(Jsons.text(s, "code"));
            }
        }
        return Optional.empty();
    }

    /** 스크립트 변경(EVT-SCR-01·설정 변경 SCRIPT)을 받으면 비운다 */
    public void invalidate() {
        cache.clear();
    }
}
