package net.java21.data2flow.flow.messaging;

import net.java21.data2flow.contracts.message.FlowDebugMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.plan.domain.DebugSample;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** 라이브 뷰 샘플 상한(BR-FLW-12, FLW-03.01·03.02, EVT-FLW-01 봉투 FlowDebugMessage v1) */
class DebugSamplerTest {

    private final MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
    private final RabbitTemplate rabbit = mock(RabbitTemplate.class);
    private final AmqpDebugSink sink = new AmqpDebugSink(rabbit, new FlowEngineProperties.Debug(5, 50), "engine-1", clock);

    private static List<DebugSample> samples(String node, int count, boolean forced) {
        List<DebugSample> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(new DebugSample(node, "m-" + i, "out", "true", Jsons.object().put("i", i), forced));
        }
        return out;
    }

    @Test
    @DisplayName("[FLW-03.01][AT-FLW-04.2] TC-FLW-061·064 BR-FLW-12 초당 500건 유입에도 노드당 샘플은 초당 5건, 디버그를 켠 노드는 50건, 다음 초에 다시 셈")
    void limitsPerNodePerSecond() {
        sink.publish(FlowFixtures.FLOW, 3, samples("n-a", 500, false));
        sink.publish(FlowFixtures.FLOW, 3, samples("n-b", 500, true));
        verify(rabbit, times(55)).send(eq("data2flow.debug"), eq("flow." + FlowFixtures.FLOW), any(Message.class));

        clock.advance(Duration.ofSeconds(1));
        sink.publish(FlowFixtures.FLOW, 3, samples("n-a", 10, false));
        verify(rabbit, times(60)).send(anyString(), anyString(), any(Message.class));
    }

    @Test
    @DisplayName("[FLW-03.02] EVT-FLW-01 node.sample 봉투: v·messageId·type·flowId·instanceId·version·sample{nodeId, messageId, direction, port, payload, masked=false}")
    void envelope() {
        sink.publish(FlowFixtures.FLOW, 7, samples("n-a", 1, false));
        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(rabbit).send(eq("data2flow.debug"), eq("flow." + FlowFixtures.FLOW), captor.capture());
        FlowDebugMessage m = MessageCodec.create().read(captor.getValue().getBody(), FlowDebugMessage.class);
        assertThat(m.type()).isEqualTo(FlowDebugMessage.Type.NODE_SAMPLE);
        assertThat(m.version()).isEqualTo(7);
        assertThat(m.instanceId()).isEqualTo("engine-1");
        assertThat(m.sample().nodeId()).isEqualTo("n-a");
        assertThat(m.sample().messageId()).isEqualTo("m-0");
        assertThat(m.sample().direction()).isEqualTo(FlowDebugMessage.NodeSample.Direction.OUT);
        assertThat(m.sample().port()).isEqualTo("true");
        assertThat(m.sample().masked()).isFalse();
    }
}
