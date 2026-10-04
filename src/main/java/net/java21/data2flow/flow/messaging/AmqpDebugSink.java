package net.java21.data2flow.flow.messaging;

import net.java21.data2flow.contracts.message.FlowDebugMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.plan.domain.DebugSample;
import net.java21.data2flow.flow.runtime.domain.DebugSink;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 라이브 뷰 발행(EVT-FLW-01, 봉투 {@link FlowDebugMessage} v1, ADR-048): topic {@code data2flow.debug}, 라우팅 키 {@code flow.{flowId}},
 * 손실 허용(확인을 기다리지 않음). 샘플({@code node.sample})은 노드당 초당 5건, 디버그 노드·overlay.debug는 50건까지(BR-FLW-12)이고,
 * 카운터({@code node.stats})는 {@code LiveStatsCollector}가 1초마다 낸다. 권한 밖 공간의 값 가리기는 받는 쪽(core-api)이 사용자 범위로
 * 한다(샘플 {@code payload.spaceId}로 판정, {@code masked}는 core가 채움).
 */
public class AmqpDebugSink implements DebugSink, Consumer<FlowDebugMessage> {

    private final RabbitTemplate rabbit;
    private final FlowEngineProperties.Debug limits;
    private final String instanceId;
    private final Clock clock;
    private final MessageCodec codec = MessageCodec.create();
    private final Map<String, long[]> windows = new ConcurrentHashMap<>();

    public AmqpDebugSink(RabbitTemplate rabbit, FlowEngineProperties.Debug limits, String instanceId, Clock clock) {
        this.rabbit = rabbit;
        this.limits = limits;
        this.instanceId = instanceId;
        this.clock = clock;
    }

    /** 이 샘플을 낼 수 있는가(노드당 초당 상한, BR-FLW-12). 넘으면 false */
    boolean admit(UUID flowId, DebugSample s) {
        long second = clock.millis() / 1000;
        int limit = s.forced() ? limits.debugSamplesPerSecond() : limits.samplesPerSecond();
        long[] w = windows.computeIfAbsent(flowId + "|" + s.nodeId() + (s.forced() ? "|d" : ""), k -> new long[]{second, 0});
        synchronized (w) {
            if (w[0] != second) {
                w[0] = second;
                w[1] = 0;
            }
            return ++w[1] <= limit;
        }
    }

    @Override
    public void publish(UUID flowId, int version, List<DebugSample> samples) {
        for (DebugSample s : samples) {
            if (!admit(flowId, s)) {
                continue;
            }
            FlowDebugMessage.NodeSample sample = new FlowDebugMessage.NodeSample(s.nodeId(), s.messageId(),
                    "in".equals(s.direction()) ? FlowDebugMessage.NodeSample.Direction.IN : FlowDebugMessage.NodeSample.Direction.OUT,
                    s.port(), s.payload(), false);
            accept(FlowDebugMessage.sample(flowId.toString(), instanceId, clock.instant(), version, sample));
        }
    }

    /** 디버그 메시지 하나를 낸다(손실 허용) */
    @Override
    public void accept(FlowDebugMessage message) {
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        props.setMessageId(message.messageId().toString());
        rabbit.send(MessagingNames.EXCHANGE_DEBUG, message.routingKey(), new Message(codec.write(message), props));
    }
}
