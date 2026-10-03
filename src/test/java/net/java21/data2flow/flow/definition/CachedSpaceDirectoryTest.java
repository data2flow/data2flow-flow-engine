package net.java21.data2flow.flow.definition;

import net.java21.data2flow.flow.definition.service.CachedSpaceDirectory;
import net.java21.data2flow.flow.definition.service.CoreFlowDirectory;
import net.java21.data2flow.flow.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** 공간 측정 기기 캐시(API-DEV-128): 메시지 처리 중에는 core를 기다리지 않고, 컴파일 때 미리 읽고, 무효화·만료되면 뒤에서 다시 읽는다 */
class CachedSpaceDirectoryTest {

    @Test
    @DisplayName("[FLW-05.01] warm으로 미리 읽고, 만료·무효화되면 이전 값을 주면서 뒤에서 다시 읽는다, core 장애는 삼킨다")
    void cache() {
        CoreFlowDirectory core = mock(CoreFlowDirectory.class);
        MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
        given(core.measuringDevices(1, 31, false)).willReturn(Set.of(5L)).willReturn(Set.of(5L, 6L));
        try (CachedSpaceDirectory dir = new CachedSpaceDirectory(core, Duration.ofMinutes(5), clock)) {
            dir.warm(1, 31, false);
            assertThat(dir.measuringDevices(1, 31, false)).containsExactly(5L);

            dir.invalidateAll();
            assertThat(dir.measuringDevices(1, 31, false)).containsExactly(5L);
            await().atMost(Duration.ofSeconds(5)).until(() -> dir.measuringDevices(1, 31, false).size() == 2);
            verify(core, times(2)).measuringDevices(1, 31, false);

            given(core.measuringDevices(1, 99, true)).willThrow(new IllegalStateException("down"));
            dir.warm(1, 99, true);
            assertThat(dir.measuringDevices(1, 99, true)).isEmpty();
        }
    }
}
