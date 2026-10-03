package net.java21.data2flow.flow.messaging;

import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.plan.domain.DebugSample;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.runtime.domain.DebugSink;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 라이브 뷰 샘플 발행(EVT-FLW-01 {@code node.sample}): topic {@code data2flow.debug}, 라우팅 키 {@code flow.{flowId}}, 손실 허용
 * (확인을 기다리지 않음). 노드당 초당 5건, 디버그 노드·overlay.debug는 50건까지(BR-FLW-12). 노드 카운터({@code node.stats})는 M4.
 */
public class AmqpDebugSink implements DebugSink {

    private final RabbitTemplate rabbit;
    private final FlowEngineProperties.Debug limits;
    private final String instanceId;
    private final Clock clock;
    private final Map<String, long[]> windows = new ConcurrentHashMap<>();

    public AmqpDebugSink(RabbitTemplate rabbit, FlowEngineProperties.Debug limits, String instanceId, Clock clock) {
        this.rabbit = rabbit;
        this.limits = limits;
        this.instanceId = instanceId;
        this.clock = clock;
    }

    @Override
    public void publish(UUID flowId, int version, List<DebugSample> samples) {
        long second = clock.millis() / 1000;
        for (DebugSample s : samples) {
            int limit = s.forced() ? limits.debugSamplesPerSecond() : limits.samplesPerSecond();
            long[] w = windows.computeIfAbsent(flowId + "|" + s.nodeId(), k -> new long[]{second, 0});
            synchronized (w) {
                if (w[0] != second) {
                    w[0] = second;
                    w[1] = 0;
                }
                if (++w[1] > limit) {
                    continue;
                }
            }
            ObjectNode body = Jsons.object();
            body.put("type", "node.sample");
            body.put("t", clock.instant().toString());
            body.put("flowId", flowId.toString());
            body.put("version", version);
            body.put("nodeId", s.nodeId());
            body.put("messageId", s.messageId());
            body.put("direction", s.direction());
            if (s.port() != null) {
                body.put("port", s.port());
            }
            body.set("payload", s.payload());
            body.put("masked", false);
            body.put("instanceId", instanceId);
            MessageProperties props = new MessageProperties();
            props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
            rabbit.send(MessagingNames.EXCHANGE_DEBUG, "flow." + flowId,
                    new Message(body.toString().getBytes(StandardCharsets.UTF_8), props));
        }
    }
}
