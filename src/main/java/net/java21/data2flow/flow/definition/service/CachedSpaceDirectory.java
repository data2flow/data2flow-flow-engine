package net.java21.data2flow.flow.definition.service;

import net.java21.data2flow.flow.node.service.SpaceDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 공간 측정 기기 캐시(core API-DEV-128). 메시지 처리 중에는 core를 부르지 않는다: 캐시에 있으면 바로 쓰고, 없거나 오래됐으면 뒤에서
 * 다시 읽는다. 컴파일할 때({@link #warm}) 미리 읽고, 기기·공간 변경(EVT-DEV-04)을 받으면 비운다. 원천은 core DB다(Redis 아님, ADR-022).
 */
public class CachedSpaceDirectory implements SpaceDirectory, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CachedSpaceDirectory.class);

    private record Entry(Set<Long> devices, Instant loadedAt) {
    }

    private final CoreFlowDirectory core;
    private final Duration ttl;
    private final Clock clock;
    private final Map<String, Entry> cache = new ConcurrentHashMap<>();
    private final Set<String> loading = ConcurrentHashMap.newKeySet();
    private final ExecutorService loader = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("space-directory").factory());

    public CachedSpaceDirectory(CoreFlowDirectory core, Duration ttl, Clock clock) {
        this.core = core;
        this.ttl = ttl;
        this.clock = clock;
    }

    private static String key(long organizationId, long spaceId, boolean descendants) {
        return organizationId + ":" + spaceId + ":" + descendants;
    }

    @Override
    public Set<Long> measuringDevices(long organizationId, long spaceId, boolean includeDescendants) {
        String key = key(organizationId, spaceId, includeDescendants);
        Entry e = cache.get(key);
        if (e == null || e.loadedAt().plus(ttl).isBefore(clock.instant())) {
            if (loading.add(key)) {
                loader.execute(() -> {
                    try {
                        load(organizationId, spaceId, includeDescendants);
                    } finally {
                        loading.remove(key);
                    }
                });
            }
        }
        return e == null ? Set.of() : e.devices();
    }

    @Override
    public void warm(long organizationId, long spaceId, boolean includeDescendants) {
        load(organizationId, spaceId, includeDescendants);
    }

    private void load(long organizationId, long spaceId, boolean includeDescendants) {
        try {
            Set<Long> devices = core.measuringDevices(organizationId, spaceId, includeDescendants);
            cache.put(key(organizationId, spaceId, includeDescendants), new Entry(devices, clock.instant()));
        } catch (RuntimeException e) {
            log.warn("공간 {}의 측정 기기를 읽지 못했습니다(메시지의 spaceId로 판정): {}", spaceId, e.getMessage());
        }
    }

    @Override
    public void invalidateAll() {
        cache.replaceAll((k, v) -> new Entry(v.devices(), Instant.EPOCH));
    }

    @Override
    public void close() {
        loader.shutdownNow();
    }
}
