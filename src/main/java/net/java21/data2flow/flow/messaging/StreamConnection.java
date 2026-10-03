package net.java21.data2flow.flow.messaging;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Environment;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;

import java.time.Duration;

/**
 * RabbitMQ Stream 연결(Environment) 하나(data2flow-pipeline과 같은 방식). 호스트·계정·vhost는 {@code spring.rabbitmq.*}, 포트는 5552.
 * 단일 노드(s4)·시험에서는 브로커가 알려 주는 주소 대신 설정한 주소로만 접속한다({@code fixed-address}).
 * {@code data2flow.telemetry}는 생산자(pipeline)가 만들므로 소비자인 이 서비스는 만들지 않는다.
 */
public class StreamConnection implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamConnection.class);

    private final RabbitProperties rabbit;
    private final FlowEngineProperties.Stream settings;
    private volatile Environment environment;

    public StreamConnection(RabbitProperties rabbit, FlowEngineProperties.Stream settings) {
        this.rabbit = rabbit;
        this.settings = settings;
    }

    public synchronized Environment environment() {
        if (environment == null) {
            String host = rabbit.getHost();
            int port = settings.port();
            var builder = Environment.builder()
                    .host(host).port(port)
                    .username(rabbit.getUsername()).password(rabbit.getPassword())
                    .virtualHost(rabbit.getVirtualHost() == null ? "/" : rabbit.getVirtualHost())
                    .recoveryBackOffDelayPolicy(BackOffDelayPolicy.fixedWithInitialDelay(Duration.ofSeconds(1), Duration.ofSeconds(2)))
                    .topologyUpdateBackOffDelayPolicy(BackOffDelayPolicy.fixedWithInitialDelay(Duration.ofSeconds(1), Duration.ofSeconds(2)));
            if (settings.fixedAddress()) {
                builder.addressResolver(address -> new Address(host, port));
            }
            environment = builder.build();
        }
        return environment;
    }

    /** 파티션 스트림 이름 {@code data2flow.telemetry-3} → 3. 모르면 -1 */
    public static int partitionIndex(String stream) {
        int dash = stream == null ? -1 : stream.lastIndexOf('-');
        if (dash < 0) {
            return -1;
        }
        try {
            return Integer.parseInt(stream.substring(dash + 1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    @Override
    public synchronized void close() {
        if (environment != null) {
            try {
                environment.close();
            } catch (RuntimeException e) {
                log.debug("Stream 연결 닫기 실패: {}", e.getMessage());
            }
            environment = null;
        }
    }
}
