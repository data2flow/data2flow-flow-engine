package net.java21.data2flow.flow.support;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * 서비스 전체 통합 시험의 공통 설정: Testcontainers PostgreSQL 18·RabbitMQ 3.13(stream)과 core 대역에 붙은 실제 엔진(스트림 소비·타이머·
 * 아웃박스 릴레이·설정 수신 모두 켬). vhost {@value #VHOST}, 파티션 3.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "management.server.port=0")
@ActiveProfiles("test")
public abstract class IntegrationTestSupport {

    public static final String VHOST = "flow-it";
    /** 시험 vhost의 스트림·큐 도구. 엔진이 뜨기 전에 Super Stream·이벤트 수신 큐를 만든다 */
    public static final TestStreams STREAMS;

    static {
        TestInfrastructure.createVhost(VHOST);
        STREAMS = new TestStreams(VHOST, 3);
        STREAMS.bindEvents("it.flow.events", "flow.apply.reported");
    }

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", TestInfrastructure.POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", TestInfrastructure.POSTGRES::getUsername);
        registry.add("spring.datasource.password", TestInfrastructure.POSTGRES::getPassword);
        registry.add("spring.rabbitmq.host", TestInfrastructure::rabbitHost);
        registry.add("spring.rabbitmq.port", TestInfrastructure::amqpPort);
        registry.add("spring.rabbitmq.username", () -> "guest");
        registry.add("spring.rabbitmq.password", () -> "guest");
        registry.add("spring.rabbitmq.virtual-host", () -> VHOST);
        registry.add("data2flow.flow.stream.port", TestInfrastructure::streamPort);
        registry.add("data2flow.flow.core.base-url", TestInfrastructure.CORE::baseUrl);
    }
}
