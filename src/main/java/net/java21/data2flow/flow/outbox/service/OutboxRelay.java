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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

/**
 * 아웃박스 릴레이(EVT-FLW-05·EVT-RUL-01, reliability-and-ha.md §2 ⑧): {@code flow_outboxes}의 보내지 않은 행을 {@code FOR UPDATE SKIP LOCKED}로
 * 가져가 보내고, <b>publisher confirm을 받은 뒤에만</b> {@code sent_at}을 채운다. 보낸 뒤 커밋 전에 죽으면 다른 인스턴스가 다시 보내지만(최소 1회)
 * 받는 쪽이 멱등 키(행동 요청)·메시지 ID(알람 신호)로 한 번만 처리한다(BR-ACT-02, BR-RUL-02).
 *
 * <ul>
 *   <li>행동 요청은 {@code data2flow.actions}(direct, 라우팅 키 command·notify·sink), 알람 신호는 {@code data2flow.events}(topic, {@code alarm.signal}).</li>
 *   <li>한 묶음(기본 50행)을 먼저 모두 보내고 확인을 모아서 기다린다(파이프라이닝). 확인을 한 건씩 기다리면 브로커 왕복이 처리량을 막는다
 *       (TC-FLW-134: 초당 200건 부하에서 감지→제어 p95 2초).</li>
 *   <li>라우팅되지 않은 메시지(mandatory 반환)·거부는 실패로 보고 1초·2배·최대 60초 뒤 다시 보낸다({@code next_attempt_at}).</li>
 *   <li>이 배포 조직의 행만 다룬다(ADR-030).</li>
 * </ul>
 */
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(60);

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

    private record Pending(OutboxRepository.OutboxRow row, CorrelationData correlation) {
    }

    /** 한 번 돈다. 보낸 수 */
    public int relayOnce(BooleanSupplier stopping) {
        Integer sent = tx.execute(status -> {
            List<OutboxRepository.OutboxRow> rows = outbox.lockUnsent(scope.organizations(), settings.batch(), clock.instant());
            List<Pending> pending = new ArrayList<>(rows.size());
            for (OutboxRepository.OutboxRow row : rows) {
                if (stopping.getAsBoolean()) {
                    break;
                }
                try {
                    pending.add(new Pending(row, send(row)));
                } catch (RuntimeException e) {
                    failed(row, e.getMessage());
                }
            }
            long deadline = System.nanoTime() + settings.confirmTimeout().toNanos();
            List<Long> sentIds = new ArrayList<>(pending.size());
            for (Pending p : pending) {
                String error = awaitConfirm(p.correlation(), deadline);
                if (error == null) {
                    sentIds.add(p.row().id());
                } else {
                    failed(p.row(), error);
                }
            }
            outbox.markSent(sentIds, clock.instant());
            return sentIds.size();
        });
        return sent == null ? 0 : sent;
    }

    private void failed(OutboxRepository.OutboxRow row, String error) {
        log.warn("아웃박스 {}({} {}) 발행 실패(다시 보냄): {}", row.id(), row.kind(), row.routingKey(), error);
        long seconds = Math.min(MAX_BACKOFF.toSeconds(), 1L << Math.min(row.attempts(), 6));
        Instant next = clock.instant().plusSeconds(seconds);
        outbox.markFailed(row.organizationId(), row.id(), error, next);
    }

    private CorrelationData send(OutboxRepository.OutboxRow row) {
        JsonNode payload = Jsons.MAPPER.readTree(row.payload());
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        props.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        String messageId = Jsons.text(payload, MessagingNames.FIELD_MESSAGE_ID);
        props.setMessageId(messageId == null ? row.idempotencyKey() : messageId);
        props.setHeader("messageId", messageId);
        props.setHeader("v", Jsons.text(payload, MessagingNames.FIELD_SCHEMA_VERSION));
        props.setHeader("organizationId", Long.toString(row.organizationId()));
        if ("EVENT".equals(row.kind())) {
            // 도메인 이벤트 봉투(MessageHeaders.of(DomainEvent)와 같은 헤더)
            props.setHeader("schema", row.routingKey());
            String occurred = Jsons.text(payload, "occurredAt");
            if (occurred != null) {
                props.setHeader("occurredAt", occurred);
            }
        } else {
            props.setHeader("schema", "action-request");
            props.setHeader("idempotencyKey", row.idempotencyKey());
        }
        CorrelationData correlation = new CorrelationData(row.idempotencyKey() + ":" + row.id());
        rabbit.send(row.exchange(), row.routingKey(), new Message(row.payload().getBytes(StandardCharsets.UTF_8), props),
                correlation);
        return correlation;
    }

    /** 확인을 기다린다. 성공이면 null, 아니면 이유 */
    private static String awaitConfirm(CorrelationData correlation, long deadlineNanos) {
        try {
            long wait = Math.max(1, deadlineNanos - System.nanoTime());
            CorrelationData.Confirm confirm = correlation.getFuture().get(wait, TimeUnit.NANOSECONDS);
            if (!confirm.ack()) {
                return "브로커가 거부했습니다: " + confirm.reason();
            }
            if (correlation.getReturned() != null) {
                return "라우팅되지 않았습니다(받을 큐 없음): " + correlation.getReturned().getReplyText();
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "발행 확인 대기가 중단되었습니다";
        } catch (ExecutionException | TimeoutException e) {
            return "발행 확인을 받지 못했습니다: " + e;
        }
    }
}
