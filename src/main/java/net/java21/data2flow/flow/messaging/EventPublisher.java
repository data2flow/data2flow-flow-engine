package net.java21.data2flow.flow.messaging;

import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.EventPayload;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.flow.common.TransientFailures.PublishFailedException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 도메인 이벤트 발행(topic {@code data2flow.events}, 라우팅 키 = {@code type}, 봉투 {@link DomainEvent}). flow-engine이 내는 것:
 * EVT-FLW-02 {@code flow.apply.reported}(M3), EVT-FLW-03 {@code flow.state.changed}(M4). 발행 확인을 기다린다.
 */
public class EventPublisher {

    private static final Duration CONFIRM_TIMEOUT = Duration.ofSeconds(10);

    private final RabbitTemplate rabbit;
    private final Clock clock;
    private final MessageCodec codec = MessageCodec.create();

    public EventPublisher(RabbitTemplate rabbit, Clock clock) {
        this.rabbit = rabbit;
        this.clock = clock;
    }

    public <P extends EventPayload> DomainEvent<P> publish(EventType type, long organizationId, P payload) {
        DomainEvent<P> event = DomainEvent.of(type, organizationId, payload, null, clock);
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        props.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        props.setMessageId(event.messageId().toString());
        MessageHeaders.of(event).forEach(props::setHeader);
        CorrelationData correlation = new CorrelationData(event.messageId().toString());
        rabbit.send(MessagingNames.EXCHANGE_EVENTS, event.type(), new Message(codec.write(event), props), correlation);
        try {
            CorrelationData.Confirm confirm = correlation.getFuture().get(CONFIRM_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (!confirm.ack()) {
                throw new PublishFailedException("이벤트 발행이 거부되었습니다: " + type.routingKey() + " " + confirm.reason(), null);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishFailedException("이벤트 발행 대기가 중단되었습니다", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new PublishFailedException("이벤트 발행 확인 실패: " + type.routingKey(), e);
        }
        return event;
    }
}
