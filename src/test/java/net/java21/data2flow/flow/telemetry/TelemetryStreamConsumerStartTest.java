package net.java21.data2flow.flow.telemetry;

import com.rabbitmq.stream.Environment;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.messaging.StreamConnection;
import net.java21.data2flow.flow.runtime.service.FlowRuntimeService;
import net.java21.data2flow.flow.telemetry.service.TelemetryStreamConsumer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * FLW-05.01: pipeline이 data2flow.telemetry를 만들기 전에 엔진이 먼저 뜨면, 스트림이 생길 때까지 기다렸다가 구독한다
 * (스트림 클라이언트는 없는 Super Stream을 파티션 0개로 구독하고 다시 찾지 않아, 이전에는 영영 텔레메트리를 받지 못했다).
 */
class TelemetryStreamConsumerStartTest {

    @Test
    @DisplayName("[FLW-05.01] 텔레메트리 Super Stream이 아직 없으면 구독하지 않고 다시 시도, 생기면 소비자를 연다")
    void waitsForSuperStream() {
        StreamConnection connection = mock(StreamConnection.class);
        Environment environment = mock(Environment.class, RETURNS_DEEP_STUBS);
        when(connection.environment()).thenReturn(environment);
        String first = SuperStreamSpec.TELEMETRY.partition(0);
        when(environment.streamExists(first)).thenReturn(false, false, true);
        FlowEngineProperties properties = new FlowEngineProperties("validate", "flow-test-0", "", true, null,
                null, null, null, null, null, null, null);
        TelemetryStreamConsumer consumer = new TelemetryStreamConsumer(connection, mock(FlowRuntimeService.class), properties);

        consumer.start();
        try {
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> verify(environment, atLeast(1)).streamExists(first));
            verify(environment, never()).consumerBuilder();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(environment, atLeast(3)).streamExists(first));
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> verify(environment, atLeast(1)).consumerBuilder());
        } finally {
            consumer.stop();
        }
    }
}
