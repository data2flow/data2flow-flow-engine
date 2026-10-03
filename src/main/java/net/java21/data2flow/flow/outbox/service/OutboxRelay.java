package net.java21.data2flow.flow.outbox.service;

import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.flow.common.DeploymentScope;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.outbox.repository.OutboxRepository;
import net.java21.data2flow.flow.plan.domain.Jsons;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * 아웃박스 릴레이(EVT-FLW-05, reliability-and-ha.md §2 ⑧): {@code flow_outboxes}의 보내지 않은 행을 {@code FOR UPDATE SKIP LOCKED}로
 * 가져가 {@code data2flow.actions}(direct, 라우팅 키 command·notify·sink)에 보내고, <b>publisher confirm을 받은 뒤에만</b> {@code sent_at}을
 * 채운다. 보낸 뒤 커밋 전에 죽으면 다른 인스턴스가 다시 보내지만(최소 1회) action이 멱등 키로 한 번만 실행한다(BR-ACT-02).
 * 라우팅되지 않은 메시지(mandatory 반환)는 실패로 보고 다시 보낸다. 이 배포 조직의 행만 다룬다(ADR-030).
 */
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final RabbitTemplate rabbit;
    private final TransactionTemplate tx;
    private final DeploymentScope scope;
    private final FlowEngineProperties.Outbox settings;
    private final Clock clock;

    public OutboxRelay(OutboxRepository outbox, RabbitTemplate rabbit, TransactionTemplate tx, DeploymentScope scope,
                       FlowEngineProperties.Outbox settings, Clock clock) {
        this.outbox = outbox;
        this.rabbit = rabbit;
        this.tx = tx;
        this.scope = scope;
        this.settings = settings;
        this.clock = clock;
    }

    /** 한 번 돈다. 보낸 수 */
    public int relayOnce(BooleanSupplier stopping) {
        Integer sent = tx.execute(status -> {
            List<OutboxRepository.OutboxRow> rows = outbox.lockUnsent(scope.organizations(), settings.batch());
            int ok = 0;
            for (OutboxRepository.OutboxRow row : rows) {
                if (stopping.getAsBoolean()) {
                    break;
                }
                try {
                    publish(row);
                    outbox.markSent(row.organizationId(), row.id(), clock.instant());
                    ok++;
                } catch (RuntimeException e) {
                    log.warn("아웃박스 {} 발행 실패(다시 보냄): {}", row.id(), e.getMessage());
                    outbox.markFailed(row.organizationId(), row.id(), e.getMessage());
                }
            }
            return ok;
        });
        return sent == null ? 0 : sent;
    }

    private void publish(OutboxRepository.OutboxRow row) {
        JsonNode payload = Jsons.MAPPER.readTree(row.payload());
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        props.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        String messageId = Jsons.text(payload, MessagingNames.FIELD_MESSAGE_ID);
        props.setMessageId(messageId == null ? row.idempotencyKey() : messageId);
        props.setHeader("messageId", messageId);
        props.setHeader("v", Jsons.text(payload, MessagingNames.FIELD_SCHEMA_VERSION));
        props.setHeader("schema", "action-request");
        props.setHeader("organizationId", Long.toString(row.organizationId()));
        props.setHeader("idempotencyKey", row.idempotencyKey());
        CorrelationData correlation = new CorrelationData(row.idempotencyKey() + ":" + row.id());
        rabbit.send(row.exchange(), row.routingKey(), new Message(row.payload().getBytes(StandardCharsets.UTF_8), props),
                correlation);
        try {
            CorrelationData.Confirm confirm = correlation.getFuture().get(settings.confirmTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
            if (!confirm.ack()) {
                throw new IllegalStateException("브로커가 거부했습니다: " + confirm.reason());
            }
            if (correlation.getReturned() != null) {
                throw new IllegalStateException("라우팅되지 않았습니다(받을 큐 없음): " + correlation.getReturned().getReplyText());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("발행 확인 대기가 중단되었습니다", e);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("발행 확인을 받지 못했습니다", e);
        }
    }
}
