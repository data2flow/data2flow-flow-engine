package net.java21.data2flow.flow.support;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.QuorumQueueSpec;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 시험용 메시징 도구: pipeline처럼 {@code data2flow.telemetry}(파티션 3)에 표준 메시지를 발행하고(라우팅 키 deviceId), action처럼
 * {@code action.commands} 큐에서 행동 요청을 꺼낸다.
 */
public final class TestStreams implements AutoCloseable {

    private static final MessageCodec CODEC = MessageCodec.create();

    private final Environment environment;
    private final Producer producer;
    private final Connection amqp;
    private final Channel channel;

    public TestStreams(String vhost, int partitions) {
        String host = TestInfrastructure.rabbitHost();
        int port = TestInfrastructure.streamPort();
        this.environment = Environment.builder().host(host).port(port).username("guest").password("guest")
                .virtualHost(vhost).addressResolver(address -> new Address(host, port)).build();
        try {
            environment.streamCreator().name(SuperStreamSpec.TELEMETRY.name()).maxAge(SuperStreamSpec.TELEMETRY.maxAge())
                    .superStream().partitions(partitions).creator().create();
        } catch (StreamException e) {
            // 이미 있음
        }
        this.producer = environment.producerBuilder().superStream(MessagingNames.STREAM_TELEMETRY)
                .routing(m -> m.getApplicationProperties().get("routingKey").toString()).producerBuilder().build();
        try {
            ConnectionFactory f = new ConnectionFactory();
            f.setHost(host);
            f.setPort(TestInfrastructure.amqpPort());
            f.setVirtualHost(vhost);
            this.amqp = f.newConnection();
            this.channel = amqp.createChannel();
            QuorumQueueSpec q = QuorumQueueSpec.ACTION_COMMANDS;
            channel.exchangeDeclare(q.exchange(), "direct", true);
            channel.exchangeDeclare(MessagingNames.EXCHANGE_DLX, "direct", true);
            channel.queueDeclare(q.name(), true, false, false, q.arguments());
            channel.queueBind(q.name(), q.exchange(), q.routingKey());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 발행하고 확인(confirm)을 기다린다 */
    public void publish(CanonicalTelemetry telemetry) {
        CompletableFuture<Boolean> confirmed = new CompletableFuture<>();
        var builder = producer.messageBuilder().properties().messageId(telemetry.messageId().toString()).messageBuilder()
                .applicationProperties();
        MessageHeaders.of(telemetry).forEach((k, v) -> builder.entry(k, v.toString()));
        builder.entry("routingKey", telemetry.routingKey());
        producer.send(builder.messageBuilder().addData(CODEC.write(telemetry)).build(), status -> confirmed.complete(status.isConfirmed()));
        try {
            if (!confirmed.get(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("data2flow.telemetry 발행 확인 실패");
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 확인을 기다리지 않고 발행한다(부하 생성). 확인되면 true로 완료 */
    public CompletableFuture<Boolean> publishAsync(CanonicalTelemetry telemetry) {
        CompletableFuture<Boolean> confirmed = new CompletableFuture<>();
        var builder = producer.messageBuilder().properties().messageId(telemetry.messageId().toString()).messageBuilder()
                .applicationProperties();
        MessageHeaders.of(telemetry).forEach((k, v) -> builder.entry(k, v.toString()));
        builder.entry("routingKey", telemetry.routingKey());
        producer.send(builder.messageBuilder().addData(CODEC.write(telemetry)).build(), status -> confirmed.complete(status.isConfirmed()));
        return confirmed;
    }

    private final List<ActionRequest> received = new java.util.concurrent.CopyOnWriteArrayList<>();

    /** action.commands 큐에 지금 있는 행동 요청을 모두 꺼내(ACK) 받은 목록에 더하고, 그 플로우의 것만 돌려준다 */
    public synchronized List<ActionRequest> commands(java.util.UUID flowId) {
        try {
            GetResponse r;
            while ((r = channel.basicGet(QuorumQueueSpec.ACTION_COMMANDS.name(), true)) != null) {
                received.add(CODEC.read(r.getBody(), ActionRequest.class));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return received.stream().filter(a -> flowId.toString().equals(a.source().flowId())).toList();
    }

    /** data2flow.events의 라우팅 키를 받는 시험 큐 */
    public void bindEvents(String queue, String routingKey) {
        try {
            channel.exchangeDeclare(MessagingNames.EXCHANGE_EVENTS, "topic", true);
            channel.queueDeclare(queue, true, false, false, null);
            channel.queueBind(queue, MessagingNames.EXCHANGE_EVENTS, routingKey);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 시험 큐에 있는 이벤트 본문을 모두 꺼낸다 */
    public List<byte[]> drain(String queue) {
        List<byte[]> out = new ArrayList<>();
        try {
            GetResponse r;
            while ((r = channel.basicGet(queue, true)) != null) {
                out.add(r.getBody());
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return out;
    }

    /** data2flow.config에 설정 변경을 낸다(core-api 역할) */
    public void publishConfig(byte[] body) {
        try {
            channel.exchangeDeclare(MessagingNames.EXCHANGE_CONFIG, "fanout", true);
            channel.basicPublish(MessagingNames.EXCHANGE_CONFIG, "", null, body);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** AMQP 채널(시험이 큐를 더 만들 때) */
    public Channel channel() {
        return channel;
    }

    @Override
    public void close() {
        try {
            channel.close();
            amqp.close();
        } catch (Exception ignored) {
            // 시험 정리
        }
        producer.close();
        environment.close();
    }
}
