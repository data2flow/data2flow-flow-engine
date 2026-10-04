package net.java21.data2flow.flow.runtime;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.dto.FlowMetricsResponse;
import net.java21.data2flow.flow.runtime.repository.MetricRepository;
import net.java21.data2flow.flow.runtime.service.FlowMetricsService;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/** 플로우 지표(FLW-05.05, API-FLW-14 내부) */
class FlowStatusMetricsTest {

    private final MutableClock clock = MutableClock.atUtc("2026-03-02T01:00:30Z");
    private final MetricRepository repository = mock(MetricRepository.class);
    private final FlowMetricsService service = new FlowMetricsService(repository, new FlowRegistry(), clock);

    @Test
    @DisplayName("[FLW-05.05][AT-FLW-10.4] TC-FLW-107 1시간: 실행 1,000·오류 20(2%), 평균·p95 소요 시간, 제어·알림·Sink 행동 수, 노드별·1분 단위")
    void oneHour() {
        Instant base = Instant.parse("2026-03-02T00:30:00Z");
        List<MetricRepository.MinuteRow> rows = new ArrayList<>();
        // 10분 동안 분마다 100건, 그중 2건 오류. 지연: 90건 5ms 이하, 8건 50ms 이하, 2건 200ms 이하
        for (int i = 0; i < 10; i++) {
            long[] buckets = new long[13];
            buckets[2] = 90;
            buckets[5] = 8;
            buckets[7] = 2;
            rows.add(new MetricRepository.MinuteRow("*", base.plus(Duration.ofMinutes(i)), 100, 2, 1, 1200, 10, 3, 5, buckets));
            rows.add(new MetricRepository.MinuteRow("n-thr00001", base.plus(Duration.ofMinutes(i)), 100, 2, 0, 300, 0, 0, 0,
                    new long[0]));
        }
        given(repository.findOrganization(FlowFixtures.FLOW)).willReturn(Optional.of(FlowFixtures.ORG));
        given(repository.rows(eq(FlowFixtures.ORG), eq(FlowFixtures.FLOW), any(), any())).willReturn(rows);

        FlowMetricsResponse r = service.metrics(FlowFixtures.FLOW, "1h", null);

        assertThat(r.summary().executions()).isEqualTo(1000);
        assertThat(r.summary().errors()).isEqualTo(20);
        assertThat(r.summary().errorRate()).isEqualTo(0.02);
        assertThat(r.summary().avgMs()).isEqualTo(12.0);
        assertThat(r.summary().p95Ms()).as("95번째는 50ms 구간").isEqualTo(50.0);
        assertThat(r.summary().actions().command()).isEqualTo(100);
        assertThat(r.summary().actions().notifyCount()).isEqualTo(30);
        assertThat(r.summary().actions().sink()).isEqualTo(50);
        assertThat(r.summary().droppedTriggers()).isEqualTo(10);
        assertThat(r.nodes()).singleElement().satisfies(n -> {
            assertThat(n.nodeId()).isEqualTo("n-thr00001");
            assertThat(n.avgMs()).isEqualTo(3.0);
        });
        assertThat(r.series()).as("1분 단위 60칸").hasSize(60);
        assertThat(r.series().stream().mapToLong(FlowMetricsResponse.Point::executions).sum()).isEqualTo(1000);
    }

    @Test
    @DisplayName("[FLW-05.05] 지표가 없는 플로우는 0, window·step 오류는 400, 24h는 기본 1시간 단위")
    void emptyAndValidation() {
        given(repository.findOrganization(any())).willReturn(Optional.empty());
        FlowMetricsResponse r = service.metrics(FlowFixtures.FLOW, "24h", null);
        assertThat(r.summary().executions()).isZero();
        assertThat(r.summary().p95Ms()).isZero();
        assertThat(r.series()).hasSize(24);
        assertThatThrownBy(() -> service.metrics(FlowFixtures.FLOW, "2h", null)).isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> service.metrics(FlowFixtures.FLOW, "1h", "5m")).isInstanceOf(BusinessException.class);
        given(repository.rows(anyLong(), any(), any(), any())).willReturn(List.of());
        assertThat(service.metrics(FlowFixtures.FLOW, "7d", "1h").series()).hasSize(168);
    }
}
